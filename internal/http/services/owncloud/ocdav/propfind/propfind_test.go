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

package propfind_test

import (
	"context"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"

	link "github.com/cs3org/go-cs3apis/cs3/sharing/link/v1beta1"
	sprovider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typesv1beta1 "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/internal/http/services/owncloud/ocdav/net"
	"github.com/cs3org/reva/internal/http/services/owncloud/ocdav/propfind"
	"github.com/cs3org/reva/internal/http/services/owncloud/ocdav/propfind/mocks"
	"github.com/cs3org/reva/pkg/rgrpc/status"
	"github.com/stretchr/testify/mock"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Propfind", func() {
	var (
		handler *propfind.Handler
		client  *mocks.GatewayClient
		ctx     context.Context

		readResponse = func(r io.Reader) (*propfind.MultiStatusResponseUnmarshalXML, string, error) {
			buf, err := ioutil.ReadAll(r)
			if err != nil {
				return nil, "", err
			}
			res := &propfind.MultiStatusResponseUnmarshalXML{}
			err = xml.Unmarshal(buf, res)
			if err != nil {
				return nil, "", err
			}

			return res, string(buf), nil
		}

		mockStat = func(ref *sprovider.Reference, info *sprovider.ResourceInfo) {
			client.On("Stat", mock.Anything, mock.MatchedBy(func(req *sprovider.StatRequest) bool {
				return (ref.ResourceId.GetOpaqueId() == "" || req.Ref.ResourceId.GetOpaqueId() == ref.ResourceId.GetOpaqueId()) &&
					(ref.Path == "" || req.Ref.Path == ref.Path)
			})).Return(&sprovider.StatResponse{
				Status: status.NewOK(ctx),
				Info:   info,
			}, nil)
		}
		mockListContainer = func(ref *sprovider.Reference, infos []*sprovider.ResourceInfo) {
			client.On("ListContainer", mock.Anything, mock.MatchedBy(func(req *sprovider.ListContainerRequest) bool {
				match := (ref.ResourceId.GetOpaqueId() == "" || req.Ref.ResourceId.GetOpaqueId() == ref.ResourceId.GetOpaqueId()) &&
					(ref.Path == "" || req.Ref.Path == ref.Path)
				return match
			})).Return(&sprovider.ListContainerResponse{
				Status: status.NewOK(ctx),
				Infos:  infos,
			}, nil)
		}

		foospace = &sprovider.StorageSpace{
			Opaque: &typesv1beta1.Opaque{
				Map: map[string]*typesv1beta1.OpaqueEntry{
					"path": {
						Decoder: "plain",
						Value:   []byte("/foo"),
					},
				},
			},
			Id:   &sprovider.StorageSpaceId{OpaqueId: "foospace"},
			Root: &sprovider.ResourceId{OpaqueId: "foospaceroot"},
			Name: "foospace",
		}
		fooquxspace = &sprovider.StorageSpace{
			Opaque: &typesv1beta1.Opaque{
				Map: map[string]*typesv1beta1.OpaqueEntry{
					"path": {
						Decoder: "plain",
						Value:   []byte("/foo/qux"),
					},
				},
			},
			Id:   &sprovider.StorageSpaceId{OpaqueId: "fooquxspace"},
			Root: &sprovider.ResourceId{OpaqueId: "fooquxspaceroot"},
			Name: "fooquxspace",
		}
		fooFileShareSpace = &sprovider.StorageSpace{
			Opaque: &typesv1beta1.Opaque{
				Map: map[string]*typesv1beta1.OpaqueEntry{
					"path": {
						Decoder: "plain",
						Value:   []byte("/foo/Shares/sharedFile"),
					},
				},
			},
			Id:   &sprovider.StorageSpaceId{OpaqueId: "fooFileShareSpace"},
			Root: &sprovider.ResourceId{OpaqueId: "sharedfile"},
			Name: "fooFileShareSpace",
		}
		fooFileShare2Space = &sprovider.StorageSpace{
			Opaque: &typesv1beta1.Opaque{
				Map: map[string]*typesv1beta1.OpaqueEntry{
					"path": {
						Decoder: "plain",
						Value:   []byte("/foo/Shares/sharedFile2"),
					},
				},
			},
			Id:   &sprovider.StorageSpaceId{OpaqueId: "fooFileShareSpace2"},
			Root: &sprovider.ResourceId{OpaqueId: "sharedfile2"},
			Name: "fooFileShareSpace2",
		}
	)

	JustBeforeEach(func() {
		ctx = context.WithValue(context.Background(), net.CtxKeyBaseURI, "http://127.0.0.1:3000")
		client = &mocks.GatewayClient{}
		handler = propfind.NewHandler("127.0.0.1:3000", func() (propfind.GatewayClient, error) {
			return client, nil
		})

		mockStat(&sprovider.Reference{ResourceId: &sprovider.ResourceId{OpaqueId: "foospaceroot"}, Path: "."},
			&sprovider.ResourceInfo{
				Type: sprovider.ResourceType_RESOURCE_TYPE_CONTAINER,
				Path: ".",
				Size: uint64(101),
			})
		mockListContainer(&sprovider.Reference{ResourceId: &sprovider.ResourceId{OpaqueId: "foospaceroot"}, Path: "."},
			[]*sprovider.ResourceInfo{
				{
					Type: sprovider.ResourceType_RESOURCE_TYPE_FILE,
					Path: "bar",
					Size: 100,
				},
				{
					Type: sprovider.ResourceType_RESOURCE_TYPE_FILE,
					Path: "baz",
					Size: 1,
				},
			})
		mockStat(&sprovider.Reference{ResourceId: &sprovider.ResourceId{OpaqueId: "foospaceroot"}, Path: "./bar"},
			&sprovider.ResourceInfo{
				Type: sprovider.ResourceType_RESOURCE_TYPE_FILE,
				Path: "./bar",
				Size: uint64(100),
			})

		mockStat(&sprovider.Reference{ResourceId: &sprovider.ResourceId{OpaqueId: "fooquxspaceroot"}, Path: "."},
			&sprovider.ResourceInfo{
				Type: sprovider.ResourceType_RESOURCE_TYPE_CONTAINER,
				Path: ".",
				Size: uint64(1000),
			})
		mockListContainer(&sprovider.Reference{ResourceId: &sprovider.ResourceId{OpaqueId: "fooquxspaceroot"}, Path: "."},
			[]*sprovider.ResourceInfo{
				{
					Type: sprovider.ResourceType_RESOURCE_TYPE_FILE,
					Path: "quux",
					Size: 1000,
				},
			})

		mockStat(&sprovider.Reference{ResourceId: &sprovider.ResourceId{OpaqueId: "sharedfile"}, Path: "."},
			&sprovider.ResourceInfo{
				Type:  sprovider.ResourceType_RESOURCE_TYPE_FILE,
				Path:  ".",
				Size:  uint64(2000),
				Mtime: &typesv1beta1.Timestamp{Seconds: 1},
			})

		mockStat(&sprovider.Reference{ResourceId: &sprovider.ResourceId{OpaqueId: "sharedfile2"}, Path: "."},
			&sprovider.ResourceInfo{
				Type:  sprovider.ResourceType_RESOURCE_TYPE_FILE,
				Path:  ".",
				Size:  uint64(2500),
				Mtime: &typesv1beta1.Timestamp{Seconds: 2},
			})

		client.On("ListPublicShares", mock.Anything, mock.Anything).Return(&link.ListPublicSharesResponse{
			Status: status.NewOK(ctx),
		}, nil)
	})

	Describe("NewHandler", func() {
		It("returns a handler", func() {
			Expect(handler).ToNot(BeNil())
		})
	})

	Describe("HandlePathPropfind", func() {
		Context("with just one space", func() {
			JustBeforeEach(func() {
				client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
					p := string(req.Opaque.Map["path"].Value)
					return p == "/" || strings.HasPrefix(p, "/foo")
				})).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{foospace},
				}, nil)
				client.On("ListStorageSpaces", mock.Anything, mock.Anything).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{},
				}, nil)
			})

			It("verifies the depth header", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo", strings.NewReader(""))
				req.Header.Set(net.HeaderDepth, "invalid")
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusBadRequest))
			})

			It("stats a path", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusMultiStatus))

				res, _, err := readResponse(rr.Result().Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(res.Responses)).To(Equal(3))

				root := res.Responses[0]
				Expect(root.Href).To(Equal("http:/127.0.0.1:3000/foo/"))
				Expect(string(root.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>101</oc:size>"))

				bar := res.Responses[1]
				Expect(bar.Href).To(Equal("http:/127.0.0.1:3000/foo/bar"))
				Expect(string(bar.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>100</d:getcontentlength>"))

				baz := res.Responses[2]
				Expect(baz.Href).To(Equal("http:/127.0.0.1:3000/foo/baz"))
				Expect(string(baz.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>1</d:getcontentlength>"))
			})

			It("stats a file", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo/bar", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusMultiStatus))

				res, _, err := readResponse(rr.Result().Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(res.Responses)).To(Equal(1))

				bar := res.Responses[0]
				Expect(bar.Href).To(Equal("http:/127.0.0.1:3000/foo/bar"))
				Expect(string(bar.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>100</d:getcontentlength>"))
			})
		})

		Context("with one nested file space", func() {
			JustBeforeEach(func() {
				client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
					return string(req.Opaque.Map["path"].Value) == "/foo" || string(req.Opaque.Map["path"].Value) == "/"
				})).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{foospace, fooFileShareSpace},
				}, nil)
				client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
					return strings.HasPrefix(string(req.Opaque.Map["path"].Value), "/foo/Shares")
				})).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{fooFileShareSpace},
				}, nil)
				client.On("ListStorageSpaces", mock.Anything, mock.Anything).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{},
				}, nil)
			})

			It("stats the parent", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusMultiStatus))

				res, _, err := readResponse(rr.Result().Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(res.Responses)).To(Equal(4))

				parent := res.Responses[0]
				Expect(parent.Href).To(Equal("http:/127.0.0.1:3000/foo/"))
				Expect(string(parent.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>2101</oc:size>"))

				sf := res.Responses[3]
				Expect(sf.Href).To(Equal("http:/127.0.0.1:3000/foo/Shares/"))
				Expect(string(sf.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>2000</oc:size>"))
			})

			It("stats the embedded space", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo/Shares/sharedFile", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusMultiStatus))

				res, _, err := readResponse(rr.Result().Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(res.Responses)).To(Equal(1))

				sf := res.Responses[0]
				Expect(sf.Href).To(Equal("http:/127.0.0.1:3000/foo/Shares/sharedFile"))
				Expect(string(sf.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>2000</d:getcontentlength>"))
				Expect(string(sf.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getlastmodified>Thu, 01 Jan 1970 00:00:01 GMT</d:getlastmodified>"))
			})
		})

		Context("with two nested file spaces", func() {
			JustBeforeEach(func() {
				client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
					return string(req.Opaque.Map["path"].Value) == "/foo" || string(req.Opaque.Map["path"].Value) == "/"
				})).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{foospace, fooFileShareSpace, fooFileShare2Space},
				}, nil)
				client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
					return strings.HasPrefix(string(req.Opaque.Map["path"].Value), "/foo/Shares")
				})).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{fooFileShareSpace, fooFileShare2Space},
				}, nil)
				client.On("ListStorageSpaces", mock.Anything, mock.Anything).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{},
				}, nil)
			})

			It("stats the parent", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusMultiStatus))

				res, _, err := readResponse(rr.Result().Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(res.Responses)).To(Equal(4))

				parent := res.Responses[0]
				Expect(parent.Href).To(Equal("http:/127.0.0.1:3000/foo/"))
				Expect(string(parent.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>4601</oc:size>"))

				shares := res.Responses[3]
				Expect(shares.Href).To(Equal("http:/127.0.0.1:3000/foo/Shares/"))
				Expect(string(shares.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>4500</oc:size>"))
				Expect(string(shares.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getlastmodified>Thu, 01 Jan 1970 00:00:02 GMT</d:getlastmodified>"))
			})
		})

		Context("with a nested directory space", func() {
			JustBeforeEach(func() {
				client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
					return string(req.Opaque.Map["path"].Value) == "/foo" || string(req.Opaque.Map["path"].Value) == "/"
				})).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{foospace, fooquxspace},
				}, nil)
				client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
					return string(req.Opaque.Map["path"].Value) == "/foo/qux"
				})).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{fooquxspace},
				}, nil)
				client.On("ListStorageSpaces", mock.Anything, mock.Anything).Return(&sprovider.ListStorageSpacesResponse{
					Status:        status.NewOK(ctx),
					StorageSpaces: []*sprovider.StorageSpace{},
				}, nil)
			})

			// Pending, the code for handling missing parents is still missing
			PIt("handles children with no parent", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusOK))
			})

			It("mounts embedded spaces", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusMultiStatus))

				res, _, err := readResponse(rr.Result().Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(res.Responses)).To(Equal(4))

				root := res.Responses[0]
				Expect(root.Href).To(Equal("http:/127.0.0.1:3000/foo/"))
				Expect(string(root.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>1101</oc:size>"))

				bar := res.Responses[1]
				Expect(bar.Href).To(Equal("http:/127.0.0.1:3000/foo/bar"))
				Expect(string(bar.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>100</d:getcontentlength>"))

				baz := res.Responses[2]
				Expect(baz.Href).To(Equal("http:/127.0.0.1:3000/foo/baz"))
				Expect(string(baz.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>1</d:getcontentlength>"))

				qux := res.Responses[3]
				Expect(qux.Href).To(Equal("http:/127.0.0.1:3000/foo/qux/"))
				Expect(string(qux.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>1000</oc:size>"))
			})

			It("stats the embedded space", func() {
				rr := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/foo/qux/", strings.NewReader(""))
				req = req.WithContext(ctx)
				Expect(err).ToNot(HaveOccurred())

				handler.HandlePathPropfind(rr, req, "/")
				Expect(rr.Code).To(Equal(http.StatusMultiStatus))

				res, _, err := readResponse(rr.Result().Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(res.Responses)).To(Equal(2))

				qux := res.Responses[0]
				Expect(qux.Href).To(Equal("http:/127.0.0.1:3000/foo/qux/"))
				Expect(string(qux.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>1000</oc:size>"))

				quux := res.Responses[1]
				Expect(quux.Href).To(Equal("http:/127.0.0.1:3000/foo/qux/quux"))
				Expect(string(quux.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>1000</d:getcontentlength>"))
			})
		})
	})

	Describe("HandleSpacesPropfind", func() {
		JustBeforeEach(func() {
			client.On("ListStorageSpaces", mock.Anything, mock.MatchedBy(func(req *sprovider.ListStorageSpacesRequest) bool {
				return req.Filters[0].Term.(*sprovider.ListStorageSpacesRequest_Filter_Id).Id.OpaqueId == "foospace"
			})).Return(&sprovider.ListStorageSpacesResponse{
				Status:        status.NewOK(ctx),
				StorageSpaces: []*sprovider.StorageSpace{foospace},
			}, nil)
		})

		It("handles invalid space ids", func() {
			client.On("ListStorageSpaces", mock.Anything, mock.Anything).Return(&sprovider.ListStorageSpacesResponse{
				Status:        status.NewOK(ctx),
				StorageSpaces: []*sprovider.StorageSpace{},
			}, nil)

			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "/", strings.NewReader(""))
			Expect(err).ToNot(HaveOccurred())

			handler.HandleSpacesPropfind(rr, req, "does-not-exist")
			Expect(rr.Code).To(Equal(http.StatusNotFound))
		})

		It("stats the space root", func() {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "/", strings.NewReader(""))
			req = req.WithContext(ctx)
			Expect(err).ToNot(HaveOccurred())

			handler.HandleSpacesPropfind(rr, req, "foospace")
			Expect(rr.Code).To(Equal(http.StatusMultiStatus))

			res, _, err := readResponse(rr.Result().Body)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(res.Responses)).To(Equal(3))

			root := res.Responses[0]
			Expect(root.Href).To(Equal("http:/127.0.0.1:3000/foospace/"))
			Expect(string(root.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>101</oc:size>"))

			bar := res.Responses[1]
			Expect(bar.Href).To(Equal("http:/127.0.0.1:3000/foospace/bar"))
			Expect(string(bar.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>100</d:getcontentlength>"))

			baz := res.Responses[2]
			Expect(baz.Href).To(Equal("http:/127.0.0.1:3000/foospace/baz"))
			Expect(string(baz.Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>1</d:getcontentlength>"))
		})

		It("stats a file", func() {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "/bar", strings.NewReader(""))
			req = req.WithContext(ctx)
			Expect(err).ToNot(HaveOccurred())

			handler.HandleSpacesPropfind(rr, req, "foospace")
			Expect(rr.Code).To(Equal(http.StatusMultiStatus))

			res, _, err := readResponse(rr.Result().Body)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(res.Responses)).To(Equal(1))
			Expect(string(res.Responses[0].Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<d:getcontentlength>100</d:getcontentlength>"))
		})

		It("stats a directory", func() {
			mockStat(&sprovider.Reference{Path: "./baz"}, &sprovider.ResourceInfo{
				Type: sprovider.ResourceType_RESOURCE_TYPE_CONTAINER,
				Size: 50,
			})
			mockListContainer(&sprovider.Reference{Path: "./baz"}, []*sprovider.ResourceInfo{
				{
					Type: sprovider.ResourceType_RESOURCE_TYPE_CONTAINER,
					Size: 50,
				},
			})

			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "/baz", strings.NewReader(""))
			req = req.WithContext(ctx)
			Expect(err).ToNot(HaveOccurred())

			handler.HandleSpacesPropfind(rr, req, "foospace")
			Expect(rr.Code).To(Equal(http.StatusMultiStatus))

			res, _, err := readResponse(rr.Result().Body)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(res.Responses)).To(Equal(2))
			Expect(string(res.Responses[0].Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>50</oc:size>"))
			Expect(string(res.Responses[1].Propstat[0].Prop[0].InnerXML)).To(ContainSubstring("<oc:size>50</oc:size>"))
		})
	})
})
