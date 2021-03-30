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

package posix

import (
	"context"
	"io"
	"net/url"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/pkg/storage"
)

type posixfs struct {
}

func NewDefault() (storage.FS, error) {
	return &posixfs{}, nil
}

func (fs *posixfs) GetHome(ctx context.Context) (string, error) {
	// TBD
	return "", nil
}

func (fs *posixfs) CreateHome(ctx context.Context) error {
	// TBD
	return nil
}

func (fs *posixfs) CreateDir(ctx context.Context, fn string) error {
	// TBD
	return nil
}

func (fs *posixfs) Delete(ctx context.Context, ref *provider.Reference) error {
	// TBD
	return nil
}

func (fs *posixfs) Move(ctx context.Context, oldRef, newRef *provider.Reference) error {
	// TBD
	return nil
}

func (fs *posixfs) GetMD(ctx context.Context, ref *provider.Reference, mdKeys []string) (*provider.ResourceInfo, error) {
	// TBD
	return nil, nil
}

func (fs *posixfs) ListFolder(ctx context.Context, ref *provider.Reference, mdKeys []string) ([]*provider.ResourceInfo, error) {
	// TBD
	return nil, nil
}

func (fs *posixfs) InitiateUpload(ctx context.Context, ref *provider.Reference, uploadLength int64, metadata map[string]string) (map[string]string, error) {
	// TBD
	return nil, nil
}

func (fs *posixfs) Upload(ctx context.Context, ref *provider.Reference, r io.ReadCloser) error {
	// TBD
	return nil
}

func (fs *posixfs) Download(ctx context.Context, ref *provider.Reference) (io.ReadCloser, error) {
	// TBD
	return nil, nil
}

func (fs *posixfs) ListRevisions(ctx context.Context, ref *provider.Reference) ([]*provider.FileVersion, error) {
	return nil, nil
	// TBD
}

func (fs *posixfs) DownloadRevision(ctx context.Context, ref *provider.Reference, key string) (io.ReadCloser, error) {
	// TBD
	return nil, nil
}

func (fs *posixfs) RestoreRevision(ctx context.Context, ref *provider.Reference, key string) error {
	// TBD
	return nil
}

func (fs *posixfs) ListRecycle(ctx context.Context) ([]*provider.RecycleItem, error) {
	// TBD
	return nil, nil
}

func (fs *posixfs) RestoreRecycleItem(ctx context.Context, key string) error {
	// TBD
	return nil
}

func (fs *posixfs) PurgeRecycleItem(ctx context.Context, key string) error {
	// TBD
	return nil
}

func (fs *posixfs) EmptyRecycle(ctx context.Context) error {
	return nil
	// TBD
}

func (fs *posixfs) GetPathByID(ctx context.Context, id *provider.ResourceId) (string, error) {
	// TBD
	return "", nil
}

func (fs *posixfs) AddGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	// TBD
	return nil
}

func (fs *posixfs) RemoveGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	// TBD
	return nil
}

func (fs *posixfs) UpdateGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	// TBD
	return nil
}

func (fs *posixfs) ListGrants(ctx context.Context, ref *provider.Reference) ([]*provider.Grant, error) {
	// TBD
	return nil, nil
}

func (fs *posixfs) GetQuota(ctx context.Context) (uint64, uint64, error) {
	// TBD
	return 0, 0, nil
}

func (fs *posixfs) CreateReference(ctx context.Context, path string, targetURI *url.URL) error {
	// TBD
	return nil
}

func (fs *posixfs) Shutdown(ctx context.Context) error {
	// TBD
	return nil
}

func (fs *posixfs) SetArbitraryMetadata(ctx context.Context, ref *provider.Reference, md *provider.ArbitraryMetadata) error {
	// TBD
	return nil
}

func (fs *posixfs) UnsetArbitraryMetadata(ctx context.Context, ref *provider.Reference, keys []string) error {
	// TBD
	return nil
}
