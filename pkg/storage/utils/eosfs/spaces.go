// Copyright 2018-2021 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package eosfs

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	ocsconv "github.com/cs3org/reva/v2/internal/http/services/owncloud/ocs/conversions"
	"github.com/cs3org/reva/v2/pkg/appctx"
	"github.com/cs3org/reva/v2/pkg/eosclient"
	"github.com/cs3org/reva/v2/pkg/errtypes"
	"github.com/cs3org/reva/v2/pkg/rgrpc/status"
	"github.com/cs3org/reva/v2/pkg/storage/utils/acl"
	"github.com/cs3org/reva/v2/pkg/storage/utils/grants"
	"github.com/cs3org/reva/v2/pkg/storage/utils/templates"
	"github.com/cs3org/reva/v2/pkg/storagespace"
	"github.com/cs3org/reva/v2/pkg/utils"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	spaceTypePersonal = "personal"
	spaceTypeProject  = "project"

	OcisPrefix           = "ocis."
	SpaceNameAttr        = OcisPrefix + "space.name"
	SpaceTypeAttr        = OcisPrefix + "space.type"
	SpaceDescriptionAttr = OcisPrefix + "space.description"
	SpaceReadmeAttr      = OcisPrefix + "space.readme"
	SpaceImageAttr       = OcisPrefix + "space.image"
)

// SpacesConfig specifies the required configuration parameters needed
// to connect to the project spaces DB
type SpacesConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	DbUsername string `mapstructure:"db_username"`
	DbPassword string `mapstructure:"db_password"`
	DbHost     string `mapstructure:"db_host"`
	DbName     string `mapstructure:"db_name"`
	DbTable    string `mapstructure:"db_table"`
	DbPort     int    `mapstructure:"db_port"`
}

var (
	egroupRegex = regexp.MustCompile(`^cernbox-project-(?P<Name>.+)-(?P<Permissions>admins|writers|readers)\z`)
)

func (fs *eosfs) ListStorageSpaces(ctx context.Context, filter []*provider.ListStorageSpacesRequest_Filter, unrestricted bool) ([]*provider.StorageSpace, error) {
	u, err := getUser(ctx)
	if err != nil {
		err = errors.Wrap(err, "eosfs: wrap: no user in ctx")
		return nil, err
	}

	spaceID, spaceType, spacePath := "", "", ""

	for i := range filter {
		switch filter[i].Type {
		case provider.ListStorageSpacesRequest_Filter_TYPE_ID:
			_, spaceID, _, _ = storagespace.SplitID(filter[i].GetId().OpaqueId)
		case provider.ListStorageSpacesRequest_Filter_TYPE_PATH:
			spacePath = filter[i].GetPath()
		case provider.ListStorageSpacesRequest_Filter_TYPE_SPACE_TYPE:
			spaceType = filter[i].GetSpaceType()
		}
	}

	if spaceType != "" && spaceType != spaceTypePersonal && spaceType != spaceTypeProject {
		spaceType = ""
	}

	cachedSpaces, err := fs.fetchCachedSpaces(ctx, u, spaceType, spaceID, spacePath)
	if err == nil {
		return cachedSpaces, nil
	}

	spaces := []*provider.StorageSpace{}

	if spaceID == "" && (spaceType == "" || spaceType == spaceTypePersonal) {
		personalSpaces, err := fs.listPersonalStorageSpaces(ctx, u, spaceID, spacePath)
		if err != nil {
			return nil, err
		}
		spaces = append(spaces, personalSpaces...)
	}
	if fs.conf.SpacesConfig.Enabled && (spaceType == "" || spaceType == spaceTypeProject) {
		projectSpaces, err := fs.listProjectStorageSpaces(ctx, u, spaceID, spacePath)
		if err != nil {
			return nil, err
		}
		spaces = append(spaces, projectSpaces...)
	}

	fs.cacheSpaces(ctx, u, spaceType, spaceID, spacePath, spaces)
	return spaces, nil
}

func (fs *eosfs) listPersonalStorageSpaces(ctx context.Context, u *userpb.User, spaceID, spacePath string) ([]*provider.StorageSpace, error) {
	var eosFileInfo *eosclient.FileInfo
	// if no spaceID and spacePath are provided, we just return the user home
	switch {
	case spaceID == "" && (spacePath == "" || spacePath == "."):
		fn, err := fs.wrapUserHomeStorageSpaceID(ctx, u, "/")
		if err != nil {
			return nil, err
		}

		auth, err := fs.getUserAuth(ctx, u, fn)
		if err != nil {
			return nil, err
		}
		eosFileInfo, err = fs.c.GetFileInfoByPath(ctx, auth, fn)
		if err != nil {
			return nil, err
		}
	case spacePath == "":
		// else, we'll stat the resource by inode
		auth, err := fs.getUserAuth(ctx, u, "")
		if err != nil {
			return nil, err
		}

		inode, err := strconv.ParseUint(spaceID, 10, 64)
		if err != nil {
			return nil, err
		}

		eosFileInfo, err = fs.c.GetFileInfoByInode(ctx, auth, inode)
		if err != nil {
			return nil, err
		}
	default:
		fn := path.Join(fs.conf.Namespace, spacePath)
		auth, err := fs.getUserAuth(ctx, u, fn)
		if err != nil {
			return nil, err
		}

		eosFileInfo, err = fs.c.GetFileInfoByPath(ctx, auth, fn)
		if err != nil {
			return nil, err
		}
	}

	md, err := fs.convertToResourceInfo(ctx, eosFileInfo, fmt.Sprintf("%d", eosFileInfo.FID), true)
	if err != nil {
		return nil, err
	}

	// If the request was for a relative ref, return just the base path
	if !strings.HasPrefix(spacePath, "/") {
		md.Path = path.Base(md.Path)
	}

	ssID, err := storagespace.FormatReference(
		&provider.Reference{
			ResourceId: &provider.ResourceId{
				SpaceId:  md.Id.SpaceId,
				OpaqueId: md.Id.OpaqueId,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return []*provider.StorageSpace{{
		Id:        &provider.StorageSpaceId{OpaqueId: ssID},
		Name:      md.Owner.OpaqueId,
		SpaceType: "personal",
		Owner:     &userpb.User{Id: md.Owner},
		Root: &provider.ResourceId{
			SpaceId:  md.Id.SpaceId,
			OpaqueId: md.Id.OpaqueId,
		},
		Mtime: &types.Timestamp{
			Seconds: eosFileInfo.MTimeSec,
			Nanos:   eosFileInfo.MTimeNanos,
		},
		Quota: &provider.Quota{},
		Opaque: &types.Opaque{
			Map: map[string]*types.OpaqueEntry{
				"path": {
					Decoder: "plain",
					Value:   []byte(md.Path),
				},
				"spaceAlias": {
					Decoder: "plain",
					Value:   []byte("personal/" + md.Owner.OpaqueId),
				},
			},
		},
	}}, nil
}

func (fs *eosfs) fileinfoToSpace(ctx context.Context, fi *eosclient.FileInfo) (*provider.StorageSpace, error) {
	relPath := strings.TrimPrefix(fi.File, fs.conf.Namespace)
	sid := fmt.Sprintf("%d", fi.FID)
	ssID, err := storagespace.FormatReference(
		&provider.Reference{
			ResourceId: &provider.ResourceId{
				SpaceId:  sid,
				OpaqueId: sid,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	owner, err := fs.getUserIDGateway(ctx, strconv.FormatUint(fi.UID, 10))
	if err != nil {
		sublog := appctx.GetLogger(ctx).With().Logger()
		sublog.Warn().Uint64("UID", fi.UID).Msg("could not lookup userid, leaving empty")
	}

	grantMap := make(map[string]*provider.ResourcePermissions, len(fi.SysACL.Entries))
	groupMap := make(map[string]struct{})
	for _, g := range fi.SysACL.Entries {
		var id string
		switch g.Type {
		case acl.TypeGroup:
			id = g.Qualifier
			groupMap[id] = struct{}{}
		case acl.TypeUser:
			grantee, err := fs.getUserIDGateway(ctx, g.Qualifier)
			if err != nil {
				sublog := appctx.GetLogger(ctx).With().Logger()
				sublog.Warn().Str("userid", g.Qualifier).Msg("could not lookup userid, leaving empty")
			}
			id = grantee.OpaqueId
		default:
			continue
		}

		grantMap[id] = grants.GetGrantPermissionSet(g.Permissions)
	}

	grantMapJSON, err := json.Marshal(grantMap)
	if err != nil {
		return nil, err
	}

	groupMapJSON, err := json.Marshal(groupMap)
	if err != nil {
		return nil, err
	}

	spaceName := fi.Attrs["user."+SpaceNameAttr]
	spaceType := fi.Attrs["user."+SpaceTypeAttr]
	space := &provider.StorageSpace{
		Id:        &provider.StorageSpaceId{OpaqueId: ssID},
		SpaceType: spaceType,
		Name:      spaceName,
		Owner:     &userpb.User{Id: owner},
		Root: &provider.ResourceId{
			SpaceId:  sid,
			OpaqueId: sid,
		},
		Mtime: &types.Timestamp{
			Seconds: fi.MTimeSec,
			Nanos:   fi.MTimeNanos,
		},
		Quota: &provider.Quota{},
		Opaque: &types.Opaque{
			Map: map[string]*types.OpaqueEntry{
				"path": {
					Decoder: "plain",
					Value:   []byte(relPath),
				},
				"spaceAlias": {
					Decoder: "plain",
					Value:   []byte("project/" + spaceName),
				},
				"description": {
					Decoder: "plain",
					Value:   []byte(fi.Attrs["user."+SpaceDescriptionAttr]),
				},
				"grants": {
					Decoder: "json",
					Value:   grantMapJSON,
				},
				"groups": {
					Decoder: "json",
					Value:   groupMapJSON,
				},
			},
		},
	}

	if spaceImage, ok := fi.Attrs["user."+SpaceImageAttr]; ok {
		space.Opaque = utils.AppendPlainToOpaque(space.Opaque, "image", storagespace.FormatResourceID(
			provider.ResourceId{StorageId: space.Root.StorageId, SpaceId: space.Root.SpaceId, OpaqueId: spaceImage},
		))
	}
	return space, nil
}

func (fs *eosfs) listProjectStorageSpaces(ctx context.Context, user *userpb.User, spaceID, spacePath string) ([]*provider.StorageSpace, error) {
	log := appctx.GetLogger(ctx)

	spaces := []*provider.StorageSpace{}
	spaceIDs := map[string]struct{}{}

	rootAuth, err := fs.getRootAuth(ctx)
	if err != nil {
		return nil, err
	}
	auth, err := fs.getUIDGateway(ctx, user.Id)
	if err != nil {
		return nil, err
	}

	if spaceID != "" {
		spaceIDs[spaceID] = struct{}{}
	} else {
		// Collect user spaces
		indexPath := path.Join(fs.conf.Namespace, "spaceIndexes", "by-user", user.Id.OpaqueId)
		fis, err := fs.c.List(ctx, rootAuth, indexPath)
		if err != nil {
			if _, ok := errors.Cause(err).(errtypes.IsNotFound); ok {
				// ignore. there are no spaces linked to this user yet
			} else {
				return nil, err
			}
		}
		for _, fi := range fis {
			base := path.Base(fi.File)
			if hiddenReg.MatchString(base) { // Do not treat hidden files/folders as space references
				continue
			}
			spaceIDs[base] = struct{}{}
		}

		// Collect group spaces
		for _, g := range user.Groups {
			indexPath := path.Join(fs.conf.Namespace, "spaceIndexes", "by-group", g)
			fis, err := fs.c.List(ctx, rootAuth, indexPath)
			if err != nil {
				if _, ok := errors.Cause(err).(errtypes.IsNotFound); ok {
					// ignore. there are no spaces linked to this group yet
					continue
				} else {
					return nil, err
				}
			}
			for _, fi := range fis {
				base := path.Base(fi.File)
				if hiddenReg.MatchString(base) { // Do not treat hidden files/folders as space references
					continue
				}
				spaceIDs[path.Base(fi.File)] = struct{}{}
			}
		}
	}

	// Read spaces
	for id, _ := range spaceIDs {
		spaceid, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			log.Error().Err(err).Str("spaceid", id).Msgf("eosfs: invalid space id")
			continue
		}
		fi, err := fs.c.GetFileInfoByInode(ctx, auth, spaceid)
		if err != nil {
			log.Error().Err(err).Uint64("spaceid", spaceid).Msgf("eosfs: error statting storage space")
			continue
		}

		space, err := fs.fileinfoToSpace(ctx, fi)
		if err != nil {
			log.Error().Err(err).Str("path", fi.File).Msgf("eosfs: error converting storage space")
			continue
		}
		spaces = append(spaces, space)

	}
	return spaces, nil
}

func (fs *eosfs) fetchCachedSpaces(ctx context.Context, user *userpb.User, spaceType, spaceID, spacePath string) ([]*provider.StorageSpace, error) {
	key := user.Id.OpaqueId + ":" + spaceType + ":" + spaceID + ":" + spacePath
	if spacesIf, err := fs.spacesCache.Get(key); err == nil {
		log := appctx.GetLogger(ctx)
		log.Info().Msgf("found cached spaces %s", key)
		return spacesIf.([]*provider.StorageSpace), nil
	}
	return nil, errtypes.NotFound("eosfs: spaces not found in cache")
}

func (fs *eosfs) cacheSpaces(ctx context.Context, user *userpb.User, spaceType, spaceID, spacePath string, spaces []*provider.StorageSpace) {
	key := user.Id.OpaqueId + ":" + spaceType + ":" + spaceID + ":" + spacePath
	_ = fs.spacesCache.SetWithExpire(key, spaces, time.Second*time.Duration(60))
}

func (fs *eosfs) wrapUserHomeStorageSpaceID(ctx context.Context, u *userpb.User, fn string) (string, error) {
	layout := templates.WithUser(u, fs.conf.UserLayout)
	internal := path.Join(fs.conf.Namespace, layout, fn)

	appctx.GetLogger(ctx).Debug().Msg("eosfs: wrap storage space id=" + fn + " internal=" + internal)
	return internal, nil
}

func (fs *eosfs) CreateStorageSpace(ctx context.Context, req *provider.CreateStorageSpaceRequest) (*provider.CreateStorageSpaceResponse, error) {
	u, err := getUser(ctx)
	if err != nil {
		err = errors.Wrap(err, "eosfs: wrap: no user in ctx")
		return nil, err
	}

	spaceToCreate := &provider.StorageSpace{Name: req.Name}
	var spacePath string
	switch req.Type {
	case spaceTypePersonal:
		// We need the unique path corresponding to the user. We assume that the username is the ID, and determine the path based on a specified template
		spacePath, err = fs.wrapUserHomeStorageSpaceID(ctx, u, "/")
		if err != nil {
			return nil, err
		}
		spaceToCreate.SpaceType = spaceTypePersonal
	case spaceTypeProject:
		projectID := uuid.New().String()
		spacePath = path.Join(fs.conf.Namespace, "spaces", pathify(projectID, 1, 1))
		spaceToCreate.SpaceType = spaceTypeProject
	default:
		return nil, errtypes.NotSupported("eosfs: creating storage spaces of specified type is not supported")
	}

	space, err := fs.readSpace(ctx, spacePath)
	if err != nil {
		space, err = fs.createOrUpdateSpace(ctx, spaceToCreate, spacePath, u)
		if err != nil {
			return nil, err
		}
	}

	return &provider.CreateStorageSpaceResponse{
		Status: &rpc.Status{
			Code: rpc.Code_CODE_OK,
		},
		StorageSpace: space,
	}, nil

	// We don't support creating any other types of shares (projects or spaces)
}

func (fs *eosfs) createOrUpdateSpace(ctx context.Context, space *provider.StorageSpace, spacePath string, owner *userpb.User) (*provider.StorageSpace, error) {
	rootAuth, err := fs.getRootAuth(ctx)
	if err != nil {
		return nil, err
	}

	_, err = fs.c.GetFileInfoByPath(ctx, rootAuth, spacePath)
	if err != nil {
		auth, err := fs.getUserAuth(ctx, owner, spacePath)
		if err != nil {
			return nil, err
		}

		err = fs.c.CreateDir(ctx, rootAuth, spacePath)
		if err != nil {
			return nil, err
		}

		err = fs.c.Chown(ctx, rootAuth, auth, spacePath)
		if err != nil {
			return nil, errors.Wrap(err, "eosfs: error chowning directory")
		}

		err = fs.c.Chmod(ctx, rootAuth, "2770", spacePath)
		if err != nil {
			return nil, errors.Wrap(err, "eosfs: error chmoding directory")
		}

		eosFileInfo, err := fs.c.GetFileInfoByPath(ctx, auth, spacePath)
		if err != nil {
			return nil, err
		}

		sid := strconv.FormatUint(eosFileInfo.Inode, 10)

		// Create index entry for the space owner
		err = fs.linkIndex(ctx, "user", owner.Id.OpaqueId, sid, spacePath)
		if err != nil {
			return nil, err
		}

		// Set initial attrs during creation
		attrs := []*eosclient.Attribute{
			{
				Type: SystemAttr,
				Key:  "mask",
				Val:  "700",
			},
			{
				Type: SystemAttr,
				Key:  "allow.oc.sync",
				Val:  "1",
			},
			{
				Type: SystemAttr,
				Key:  "mtime.propagation",
				Val:  "1",
			},
			{
				Type: SystemAttr,
				Key:  "forced.atomic",
				Val:  "1",
			},
			{
				Type: UserAttr,
				Key:  SpaceTypeAttr,
				Val:  space.SpaceType,
			},
		}

		for _, attr := range attrs {
			err = fs.c.SetAttr(ctx, rootAuth, attr, false, false, spacePath)
			if err != nil {
				return nil, errors.Wrap(err, "eosfs: error setting attribute")
			}
		}

		if space.SpaceType != spaceTypePersonal {
			if err := fs.AddGrant(ctx, &provider.Reference{
				ResourceId: &provider.ResourceId{
					SpaceId:  sid,
					OpaqueId: sid,
				},
			}, &provider.Grant{
				Grantee: &provider.Grantee{
					Type: provider.GranteeType_GRANTEE_TYPE_USER,
					Id: &provider.Grantee_UserId{
						UserId: owner.Id,
					},
				},
				Permissions: ocsconv.NewManagerRole().CS3ResourcePermissions(),
			}); err != nil {
				return nil, err
			}
		}
	}

	// Set/Update changed attributes
	attrs := []*eosclient.Attribute{}
	if space.Name != "" {
		attrs = append(attrs, &eosclient.Attribute{
			Type: UserAttr,
			Key:  SpaceNameAttr,
			Val:  space.Name,
		})
	}

	if space.Opaque != nil {
		if image := utils.ReadPlainFromOpaque(space.Opaque, "image"); image != "" {
			imageID, err := storagespace.ParseID(image)
			if err != nil {
				return nil, errors.Wrap(err, "eosfs: error parsing the image id")
			}
			attrs = append(attrs, &eosclient.Attribute{
				Type: UserAttr,
				Key:  SpaceImageAttr,
				Val:  imageID.OpaqueId,
			})
		}
		if description := utils.ReadPlainFromOpaque(space.Opaque, "description"); description != "" {
			attrs = append(attrs, &eosclient.Attribute{
				Type: UserAttr,
				Key:  SpaceDescriptionAttr,
				Val:  description,
			})
		}
	}

	for _, attr := range attrs {
		err = fs.c.SetAttr(ctx, rootAuth, attr, false, false, spacePath)
		if err != nil {
			return nil, errors.Wrap(err, "eosfs: error setting attribute")
		}
	}

	return fs.readSpace(ctx, spacePath)
}

func (fs *eosfs) readSpace(ctx context.Context, spacePath string) (*provider.StorageSpace, error) {
	rootAuth, err := fs.getRootAuth(ctx)
	if err != nil {
		return nil, err
	}

	eosFileInfo, err := fs.c.GetFileInfoByPath(ctx, rootAuth, spacePath)
	if err != nil {
		return nil, err
	}

	return fs.fileinfoToSpace(ctx, eosFileInfo)
}

func (fs *eosfs) UpdateStorageSpace(ctx context.Context, req *provider.UpdateStorageSpaceRequest) (*provider.UpdateStorageSpaceResponse, error) {
	auth, err := fs.getRootAuth(ctx)
	if err != nil {
		return nil, err
	}

	id, err := storagespace.ParseID(req.StorageSpace.GetId().GetOpaqueId())
	if err != nil {
		return nil, err
	}

	inode, err := strconv.ParseUint(id.SpaceId, 10, 64)
	if err != nil {
		return nil, err
	}

	eosFileInfo, err := fs.c.GetFileInfoByInode(ctx, auth, inode)
	if err != nil {
		return nil, errors.Wrap(err, "eosfs: error getting file info by inode")
	}

	owner, err := fs.getUserIDGateway(ctx, strconv.FormatUint(eosFileInfo.UID, 10))
	if err != nil {
		return nil, err
	}

	space, err := fs.createOrUpdateSpace(ctx, req.StorageSpace, eosFileInfo.File, &userpb.User{Id: owner})
	if err != nil {
		return nil, err
	}
	return &provider.UpdateStorageSpaceResponse{
		Status:       status.NewOK(ctx),
		StorageSpace: space,
	}, nil
}

func (fs *eosfs) DeleteStorageSpace(ctx context.Context, req *provider.DeleteStorageSpaceRequest) error {
	return errtypes.NotSupported("delete storage spaces")
}

func (fs *eosfs) unlinkIndex(ctx context.Context, index, value, id string) error {
	rootAuth, err := fs.getRootAuth(ctx)
	if err != nil {
		return err
	}

	indexPath := path.Join(fs.conf.Namespace, "spaceIndexes", "by-"+index, value, id)
	return fs.c.Remove(ctx, rootAuth, indexPath, true)
}

func (fs *eosfs) linkIndex(ctx context.Context, index, value, id, target string) error {
	indexPath := path.Join(fs.conf.Namespace, "spaceIndexes", "by-"+index, value, id)

	rootAuth, err := fs.getRootAuth(ctx)
	if err != nil {
		return err
	}
	err = fs.c.Symlink(ctx, rootAuth, target, indexPath)
	if err != nil {
		if _, ok := errors.Cause(err).(errtypes.AlreadyExists); ok {
			// ignore. there are no spaces linked to this user yet
			return nil
		} else {
			err = fs.c.CreateDir(ctx, rootAuth, path.Dir(indexPath))
			if err != nil {
				return err
			}
			return fs.c.Symlink(ctx, rootAuth, target, indexPath)
		}
	}
	return nil
}

// pathify segments the beginning of a string into depth segments of width length
// pathify("aabbccdd", 3, 1) will return "a/a/b/bccdd"
func pathify(id string, depth, width int) string {
	b := strings.Builder{}
	i := 0
	for ; i < depth; i++ {
		if len(id) <= i*width+width {
			break
		}
		b.WriteString(id[i*width : i*width+width])
		b.WriteRune(filepath.Separator)
	}
	b.WriteString(id[i*width:])
	return b.String()
}
