package spaceidindex

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/mtimesyncedcache"
	"github.com/pkg/errors"
	"github.com/rogpeppe/go-internal/lockedfile"
	"github.com/shamaton/msgpack/v2"
)

type Index struct {
	root  string
	name  string
	cache mtimesyncedcache.Cache[string, map[string][]byte]
}

type readWriteCloseSeekTruncater interface {
	io.ReadWriteCloser
	io.Seeker
	Truncate(int64) error
}

func New(root, index string) *Index {
	return &Index{
		root: root,
		name: index,
	}
}

func (i *Index) Load(index string) (map[string][]byte, error) {
	indexPath := filepath.Join(i.root, i.name, index+".mpk")
	fi, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}
	return i.readSpaceIndex(indexPath, i.name+":"+index, fi.ModTime())
}

// func (i *Index) RemoveFromIndex(key, entry string) os.LinkError {

func (i *Index) Add(index, key string, value []byte) error {
	return i.UpdateIndex(index, map[string][]byte{key: value}, []string{})
}

func (i *Index) Remove(index, key string) error {
	return i.UpdateIndex(index, map[string][]byte{}, []string{key})
}

func (i *Index) UpdateIndex(index string, addLinks map[string][]byte, removeLinks []string) error {
	indexPath := filepath.Join(i.root, i.name, index+".mpk")

	var err error
	// aquire writelock
	var f readWriteCloseSeekTruncater
	f, err = lockedfile.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return errors.Wrap(err, "unable to lock index to write")
	}
	defer func() {
		rerr := f.Close()

		// if err is non nil we do not overwrite that
		if err == nil {
			err = rerr
		}
	}()

	// Read current state
	msgBytes, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	links := map[string][]byte{}
	if len(msgBytes) > 0 {
		err = msgpack.Unmarshal(msgBytes, &links)
		if err != nil {
			return err
		}
	}

	// set new metadata
	for key, val := range addLinks {
		links[key] = val
	}
	for _, key := range removeLinks {
		delete(links, key)
	}
	// Truncate file
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = f.Truncate(0)
	if err != nil {
		return err
	}

	// Write new metadata to file
	d, err := msgpack.Marshal(links)
	if err != nil {
		return errors.Wrap(err, "unable to marshal index")
	}
	_, err = f.Write(d)
	if err != nil {
		return errors.Wrap(err, "unable to write index")
	}
	return nil
}

func (i *Index) readSpaceIndex(indexPath, cacheKey string, mtime time.Time) (map[string][]byte, error) {
	return i.cache.LoadOrStore(cacheKey, mtime, func() (map[string][]byte, error) {
		// Acquire a read log on the index file
		f, err := lockedfile.Open(indexPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to lock index to read")
		}
		defer func() {
			rerr := f.Close()

			// if err is non nil we do not overwrite that
			if err == nil {
				err = rerr
			}
		}()

		// Read current state
		msgBytes, err := io.ReadAll(f)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read index")
		}
		links := map[string][]byte{}
		if len(msgBytes) > 0 {
			err = msgpack.Unmarshal(msgBytes, &links)
			if err != nil {
				return nil, errors.Wrap(err, "unable to parse index")
			}
		}
		return links, nil
	})
}
