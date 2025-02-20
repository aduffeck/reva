package metadata

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/renameio/v2"
	"github.com/pkg/errors"
	"github.com/pkg/xattr"
	"github.com/rogpeppe/go-internal/lockedfile"
	"github.com/shamaton/msgpack/v2"

	"github.com/opencloud-eu/reva/v2/pkg/storage/cache"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/metadata/prefixes"
	"github.com/opencloud-eu/reva/v2/pkg/storage/utils/filelocks"
)

var _grantsOffloadedAttr = prefixes.OcPrefix + "grants_offloaded"

type MetadataPathFunc func(MetadataNode) string

// HybridBackend stores the file attributes in extended attributes
type HybridBackend struct {
	offloadLimit     int
	metaCache        cache.FileMetadataCache
	metadataPathFunc MetadataPathFunc
}

// NewMessageBackend returns a new HybridBackend instance
func NewHybridBackend(offloadLimit int, metadataPathFunc MetadataPathFunc, o cache.Config) HybridBackend {
	return HybridBackend{
		offloadLimit:     offloadLimit,
		metaCache:        cache.GetFileMetadataCache(o),
		metadataPathFunc: metadataPathFunc,
	}
}

// Name returns the name of the backend
func (HybridBackend) Name() string { return "hybrid" }

// IdentifyPath returns the space id, node id and mtime of a file
func (b HybridBackend) IdentifyPath(_ context.Context, path string) (string, string, time.Time, error) {
	spaceID, _ := xattr.Get(path, prefixes.SpaceIDAttr)
	id, _ := xattr.Get(path, prefixes.IDAttr)

	mtimeAttr, _ := xattr.Get(path, prefixes.MTimeAttr)
	mtime, _ := time.Parse(time.RFC3339Nano, string(mtimeAttr))
	return string(spaceID), string(id), mtime, nil
}

// Get an extended attribute value for the given key
// No file locking is involved here as reading a single xattr is
// considered to be atomic.
func (b HybridBackend) Get(ctx context.Context, n MetadataNode, key string) ([]byte, error) {
	attribs := map[string][]byte{}
	err := b.metaCache.PullFromCache(b.cacheKey(n), &attribs)
	if err == nil && len(attribs[key]) > 0 {
		return attribs[key], err
	}

	if isGrantAttribute(key) {
		// check if grants are offloaded
		offloaded, err := xattr.Get(n.InternalPath(), _grantsOffloadedAttr)
		if err == nil && string(offloaded) == "1" {
			msgpackAttribs := map[string][]byte{}
			msgBytes, err := os.ReadFile(b.MetadataPath(n))
			if err != nil {
				return nil, err
			}
			err = msgpack.Unmarshal(msgBytes, &msgpackAttribs)
			if err != nil {
				return nil, err
			}
			return msgpackAttribs[key], nil
		}
	}
	return xattr.Get(n.InternalPath(), key)
}

// GetInt64 reads a string as int64 from the xattrs
func (b HybridBackend) GetInt64(ctx context.Context, n MetadataNode, key string) (int64, error) {
	attr, err := b.Get(ctx, n, key)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(string(attr), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// List retrieves a list of names of extended attributes associated with the
// given path in the file system.
func (b HybridBackend) List(ctx context.Context, n MetadataNode) (attribs []string, err error) {
	return b.list(ctx, n, true)
}

func (b HybridBackend) list(ctx context.Context, n MetadataNode, acquireLock bool) (attribs []string, err error) {
	filePath := n.InternalPath()
	attrs, err := xattr.List(filePath)
	if err == nil {
		return attrs, nil
	}

	// listing xattrs failed, try again, either with lock or without
	if acquireLock {
		f, err := lockedfile.OpenFile(filePath+filelocks.LockFileSuffix, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		defer cleanupLockfile(ctx, f)

	}
	return xattr.List(filePath)
}

// All reads all extended attributes for a node, protected by a
// shared file lock
func (b HybridBackend) All(ctx context.Context, n MetadataNode) (map[string][]byte, error) {
	return b.getAll(ctx, n, false, false, true)
}

func (b HybridBackend) getAll(ctx context.Context, n MetadataNode, skipCache, skipOffloaded, acquireLock bool) (map[string][]byte, error) {
	attribs := map[string][]byte{}

	if !skipCache {
		err := b.metaCache.PullFromCache(b.cacheKey(n), &attribs)
		if err == nil {
			return attribs, err
		}
	}

	attrNames, err := b.list(ctx, n, acquireLock)
	if err != nil {
		return nil, err
	}

	if len(attrNames) == 0 {
		return attribs, nil
	}

	var (
		xerrs = 0
		xerr  error
	)
	// error handling: Count if there are errors while reading all attribs.
	// if there were any, return an error.
	attribs = make(map[string][]byte, len(attrNames))
	path := n.InternalPath()
	for _, name := range attrNames {
		var val []byte
		if val, xerr = xattr.Get(path, name); xerr != nil && !IsAttrUnset(xerr) {
			xerrs++
		} else {
			attribs[name] = val
		}
	}

	if xerrs > 0 {
		return nil, errors.Wrap(xerr, "Failed to read all xattrs")
	}

	// merge the attributes from the offload file
	offloaded, err := xattr.Get(path, _grantsOffloadedAttr)
	if skipOffloaded == false && err == nil && string(offloaded) == "1" {
		msgpackAttribs := map[string][]byte{}
		msgBytes, err := os.ReadFile(b.MetadataPath(n))
		if err != nil {
			return nil, err
		}
		err = msgpack.Unmarshal(msgBytes, &msgpackAttribs)
		if err != nil {
			return nil, err
		}
		for key, val := range msgpackAttribs {
			attribs[key] = val
		}
	}

	err = b.metaCache.PushToCache(b.cacheKey(n), attribs)
	if err != nil {
		return nil, err
	}

	return attribs, nil
}

// Set sets one attribute for the given path
func (b HybridBackend) Set(ctx context.Context, n MetadataNode, key string, val []byte) (err error) {
	return b.SetMultiple(ctx, n, map[string][]byte{key: val}, true)
}

// SetMultiple sets a set of attribute for the given path
func (b HybridBackend) SetMultiple(ctx context.Context, n MetadataNode, attribs map[string][]byte, acquireLock bool) (err error) {
	path := n.InternalPath()
	if acquireLock {
		err := os.MkdirAll(filepath.Dir(path), 0600)
		if err != nil {
			return err
		}
		lockedFile, err := lockedfile.OpenFile(b.LockfilePath(n), os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer cleanupLockfile(ctx, lockedFile)
	}

	offloaded, err := xattr.Get(path, _grantsOffloadedAttr)
	if err == nil && string(offloaded) == "1" {
		// already offloaded -> write to messagepack
		metaPath := b.MetadataPath(n)
		var msgBytes []byte
		msgBytes, err = os.ReadFile(metaPath)

		attribs := map[string][]byte{}
		switch {
		case err != nil:
			if !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		default:
			err = msgpack.Unmarshal(msgBytes, &attribs)
			if err != nil {
				return err
			}
		}

		// prepare metadata
		for key, val := range attribs {
			attribs[key] = val
		}
		var d []byte
		d, err = msgpack.Marshal(attribs)
		if err != nil {
			return err
		}

		// overwrite file atomically
		err = renameio.WriteFile(metaPath, d, 0600)
		if err != nil {
			return err
		}
	} else {
		xerrs := 0
		var xerr error
		// error handling: Count if there are errors while setting the attribs.
		// if there were any, return an error.
		for key, val := range attribs {
			if xerr = xattr.Set(path, key, val); xerr != nil {
				// log
				xerrs++
			}
		}
		if xerrs > 0 {
			return errors.Wrap(xerr, "Failed to set all xattrs")
		}
	}

	attribs, err = b.getAll(ctx, n, true, false, false)
	if err != nil {
		return err
	}
	err = b.metaCache.PushToCache(b.cacheKey(n), attribs)
	if err != nil {
		return err
	}

	// offload if the grant size exceeds the limit
	grantSize := 0
	for key := range attribs {
		if isGrantAttribute(key) {
			grantSize += len(attribs[key]) + len(key)
		}
	}
	if grantSize > b.offloadLimit && string(offloaded) != "1" {
		return b.offloadMetadata(ctx, n)
	}

	return nil
}

func (b HybridBackend) offloadMetadata(ctx context.Context, n MetadataNode) error {
	path := n.InternalPath()
	msgpackAttribs := map[string][]byte{}
	xerrs := 0
	var xerr error

	// collect attributes to move
	existingAttribs, err := b.getAll(ctx, n, true, true, false)
	if err != nil {
		return err
	}
	for key, val := range existingAttribs {
		if isGrantAttribute(key) {
			msgpackAttribs[key] = val
			xerr = xattr.Remove(path, key)
			if err != nil {
				xerrs++
			}
		}
	}

	var d []byte
	d, err = msgpack.Marshal(msgpackAttribs)
	if err != nil {
		return err
	}
	err = renameio.WriteFile(b.MetadataPath(n), d, 0600)
	if err != nil {
		return err
	}

	// set the grants offloaded attribute
	err = xattr.Set(path, _grantsOffloadedAttr, []byte("1"))
	if err != nil {
		return err
	}
	// remove grants from xattrs
	for key, val := range existingAttribs {
		if isGrantAttribute(key) {
			msgpackAttribs[key] = val
			xerr = xattr.Remove(path, key)
			if err != nil {
				xerrs++
			}
		}
	}
	if xerrs > 0 {
		return errors.Wrap(xerr, "Failed to remove xattrs while offloading")
	}

	return nil
}

// Remove an extended attribute key
func (b HybridBackend) Remove(ctx context.Context, n MetadataNode, key string, acquireLock bool) error {
	path := n.InternalPath()
	if acquireLock {
		lockedFile, err := lockedfile.OpenFile(path+filelocks.LockFileSuffix, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer cleanupLockfile(ctx, lockedFile)
	}

	err := xattr.Remove(path, key)
	if err != nil {
		return err
	}
	attribs, err := b.getAll(ctx, n, true, false, false)
	if err != nil {
		return err
	}
	return b.metaCache.PushToCache(b.cacheKey(n), attribs)
}

// IsMetaFile returns whether the given path represents a meta file
func (HybridBackend) IsMetaFile(path string) bool { return strings.HasSuffix(path, ".meta.lock") }

// Purge purges the data of a given path
func (b HybridBackend) Purge(ctx context.Context, n MetadataNode) error {
	path := n.InternalPath()
	_, err := os.Stat(path)
	if err == nil {
		attribs, err := b.getAll(ctx, n, true, false, true)
		if err != nil {
			return err
		}

		for attr := range attribs {
			if strings.HasPrefix(attr, prefixes.OcPrefix) {
				err := xattr.Remove(path, attr)
				if err != nil {
					return err
				}
			}
		}
	}

	return b.metaCache.RemoveMetadata(b.cacheKey(n))
}

// Rename moves the data for a given path to a new path
func (b HybridBackend) Rename(oldNode, newNode MetadataNode) error {
	data := map[string][]byte{}
	err := b.metaCache.PullFromCache(b.cacheKey(oldNode), &data)
	if err == nil {
		err = b.metaCache.PushToCache(b.cacheKey(newNode), data)
		if err != nil {
			return err
		}
	}
	return b.metaCache.RemoveMetadata(b.cacheKey(oldNode))
}

// MetadataPath returns the path of the file holding the metadata for the given path
func (b HybridBackend) MetadataPath(n MetadataNode) string { return b.metadataPathFunc(n) }

// LockfilePath returns the path of the lock file
func (HybridBackend) LockfilePath(n MetadataNode) string { return n.InternalPath() + ".mlock" }

// Lock locks the metadata for the given path
func (b HybridBackend) Lock(n MetadataNode) (UnlockFunc, error) {
	metaLockPath := b.LockfilePath(n)
	mlock, err := lockedfile.OpenFile(metaLockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return func() error {
		err := mlock.Close()
		if err != nil {
			return err
		}
		return os.Remove(metaLockPath)
	}, nil
}

// AllWithLockedSource reads all extended attributes from the given reader.
// The path argument is used for storing the data in the cache
func (b HybridBackend) AllWithLockedSource(ctx context.Context, n MetadataNode, _ io.Reader) (map[string][]byte, error) {
	return b.All(ctx, n)
}

func (b HybridBackend) cacheKey(n MetadataNode) string {
	// rootPath is guaranteed to have no trailing slash
	// the cache key shouldn't begin with a slash as some stores drop it which can cause
	// confusion
	return n.GetSpaceID() + "/" + n.GetID()
}

func isGrantAttribute(key string) bool {
	return strings.HasPrefix(key, prefixes.GrantPrefix)
}
