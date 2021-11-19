package gateway_test

import (
	"context"
	"strconv"
	"strings"

	"github.com/stretchr/testify/mock"
	grpc "google.golang.org/grpc"

	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	storage "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	registry "github.com/cs3org/go-cs3apis/cs3/storage/registry/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/internal/grpc/services/gateway"
	gatewaymocks "github.com/cs3org/reva/internal/grpc/services/gateway/mocks"
	"github.com/cs3org/reva/pkg/rgrpc/status"
	"github.com/cs3org/reva/pkg/sharedconf"
	_ "github.com/cs3org/reva/pkg/token/manager/loader"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storageprovider", func() {
	var (
		gw             *gateway.Gateway
		config         map[string]interface{}
		pm             *gatewaymocks.PoolManager
		registryClient *gatewaymocks.StorageRegistryAPIClient

		ctx                       = context.Background()
		rootProviderClientAddress = "192.168.0.10:10000"
		rootProviderInfo          = &registry.ProviderInfo{
			Address:      rootProviderClientAddress,
			ProviderId:   "root",
			ProviderPath: "/",
		}
		rootProviderClient = &gatewaymocks.StorageProviderAPIClient{}
		rootMTime          *types.Timestamp

		fooProviderClientAddress = "192.168.0.10:10001"
		fooProviderInfo          = &registry.ProviderInfo{
			Address:      fooProviderClientAddress,
			ProviderId:   "foo",
			ProviderPath: "/foo",
		}
		fooProviderClient = &gatewaymocks.StorageProviderAPIClient{}
		fooMTime          *types.Timestamp

		mountedProviders []*registry.ProviderInfo
		mountProvider    = func(info *registry.ProviderInfo) {
			mountedProviders = append(mountedProviders, info)
		}
	)

	BeforeEach(func() {
		mountedProviders = []*registry.ProviderInfo{}
		rootMTime = &types.Timestamp{Seconds: 0}
		fooMTime = &types.Timestamp{Seconds: 100}

		rootProviderClient.On("ListContainer", mock.Anything, mock.Anything).Return(
			&storage.ListContainerResponse{
				Status: status.NewOK(ctx),
				Infos: []*storage.ResourceInfo{
					{
						Type: storage.ResourceType_RESOURCE_TYPE_FILE,
						Id:   &storage.ResourceId{StorageId: "storage", OpaqueId: "root1"},
						Path: "root1.txt",
						Size: 1,
					},
				},
			}, nil,
		)
		rootProviderClient.On("Stat", mock.Anything, mock.Anything).Return(
			func(_ context.Context, req *storage.StatRequest, _ ...grpc.CallOption) *storage.StatResponse {
				return &storage.StatResponse{
					Status: status.NewOK(ctx),
					Info: &storage.ResourceInfo{
						Type:  storage.ResourceType_RESOURCE_TYPE_CONTAINER,
						Id:    &storage.ResourceId{StorageId: "storage", OpaqueId: "root"},
						Path:  "/",
						Size:  1,
						Mtime: rootMTime,
						Etag:  strconv.Itoa(int(rootMTime.Seconds)),
					},
				}
			}, nil,
		)
		fooProviderClient.On("ListContainer", mock.Anything, mock.Anything).Return(
			&storage.ListContainerResponse{
				Status: status.NewOK(ctx),
				Infos: []*storage.ResourceInfo{
					{
						Type: storage.ResourceType_RESOURCE_TYPE_FILE,
						Id:   &storage.ResourceId{StorageId: "storage", OpaqueId: "foo1"},
						Path: "foo1.txt",
						Size: 100,
					},
				},
			}, nil,
		)
		fooProviderClient.On("Stat", mock.Anything, mock.Anything).Return(
			func(_ context.Context, req *storage.StatRequest, _ ...grpc.CallOption) *storage.StatResponse {
				return &storage.StatResponse{
					Status: status.NewOK(ctx),
					Info: &storage.ResourceInfo{
						Type:  storage.ResourceType_RESOURCE_TYPE_CONTAINER,
						Id:    &storage.ResourceId{StorageId: "storage", OpaqueId: "foo"},
						Path:  "",
						Size:  100,
						Mtime: fooMTime,
						Etag:  strconv.Itoa(int(rootMTime.Seconds)),
					},
				}
			}, nil,
		)
	})

	JustBeforeEach(func() {
		config = map[string]interface{}{
			"shared": map[string]interface{}{
				"jwt_secret": "random_secret",
			},
		}

		var err error
		err = sharedconf.Decode(config["shared"])
		Expect(err).ToNot(HaveOccurred())

		registryClient = &gatewaymocks.StorageRegistryAPIClient{}
		registryClient.On("GetStorageProviders", mock.Anything, mock.Anything).Return(
			func(_ context.Context, req *registry.GetStorageProvidersRequest, _ ...grpc.CallOption) *registry.GetStorageProvidersResponse {
				providers := []*registry.ProviderInfo{}
				for _, p := range mountedProviders {
					if strings.HasPrefix(p.ProviderPath, req.Ref.Path) {
						providers = append(providers, p)
					}
				}
				return &registry.GetStorageProvidersResponse{
					Status:    status.NewOK(ctx),
					Providers: providers,
				}
			}, nil)

		pm = &gatewaymocks.PoolManager{}
		pm.On("GetStorageRegistryClient", mock.Anything).Return(registryClient, nil)
		pm.On("GetStorageProviderServiceClient", mock.Anything).Return(
			func(address string) provider.ProviderAPIClient {
				switch address {
				case rootProviderClientAddress:
					return rootProviderClient
				case fooProviderClientAddress:
					return fooProviderClient
				default:
					return nil
				}
			}, nil,
		)

		svc, err := gateway.New(config, pm, nil)
		Expect(err).ToNot(HaveOccurred())
		gw = svc.(*gateway.Gateway)
	})

	Context("with a root provider and another provider serving a virtual view", func() {
		BeforeEach(func() {
			mountProvider(rootProviderInfo)
			mountProvider(fooProviderInfo)
		})

		Describe("Stat", func() {
			It("stats the root", func() {
				req := &provider.StatRequest{
					Ref: &provider.Reference{Path: "/"},
				}
				res, err := gw.Stat(ctx, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(res.Status.Code).To(Equal(rpc.Code_CODE_OK))

				Expect(res.Info.Mtime.Seconds).To(Equal(uint64(100)))
				Expect(res.Info.Size).To(Equal(uint64(101)))
			})

			It("updates the root mtime and etag", func() {
				req := &provider.StatRequest{
					Ref: &provider.Reference{Path: "/"},
				}
				res, err := gw.Stat(ctx, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(res.Status.Code).To(Equal(rpc.Code_CODE_OK))

				Expect(res.Info.Mtime.Seconds).To(Equal(uint64(100)))
				Expect(res.Info.Size).To(Equal(uint64(101)))
				etag := res.Info.Etag

				rootMTime = &types.Timestamp{Seconds: 10000}
				res, err = gw.Stat(ctx, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(res.Status.Code).To(Equal(rpc.Code_CODE_OK))
				Expect(res.Info.Mtime.Seconds).To(Equal(uint64(10000)))
				Expect(res.Info.Etag).ToNot(Equal(etag))
			})

			It("updates the root etag when an embedded mount is updated", func() {
				req := &provider.StatRequest{
					Ref: &provider.Reference{Path: "/"},
				}
				res, err := gw.Stat(ctx, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(res.Status.Code).To(Equal(rpc.Code_CODE_OK))

				Expect(res.Info.Mtime.Seconds).To(Equal(uint64(100)))
				Expect(res.Info.Size).To(Equal(uint64(101)))
				etag := res.Info.Etag

				fooMTime = &types.Timestamp{Seconds: 20000}
				res, err = gw.Stat(ctx, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(res.Status.Code).To(Equal(rpc.Code_CODE_OK))
				Expect(res.Info.Etag).ToNot(Equal(etag))
			})
		})

		Describe("ListContainer", func() {
			It("lists the deeper path", func() {
				req := &provider.ListContainerRequest{
					Ref: &provider.Reference{Path: "/foo"},
				}
				res, err := gw.ListContainer(ctx, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(res.Status.Code).To(Equal(rpc.Code_CODE_OK))

				Expect(len(res.Infos)).To(Equal(1))
				info := res.Infos[0]
				Expect(info).ToNot(BeNil())
				Expect(info.Path).To(Equal("/foo/foo1.txt"))
			})

			It("lists the root", func() {
				req := &provider.ListContainerRequest{
					Ref: &provider.Reference{Path: "/"},
				}
				res, err := gw.ListContainer(ctx, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(res.Status.Code).To(Equal(rpc.Code_CODE_OK))

				Expect(len(res.Infos)).To(Equal(2))
				info := res.Infos[0]
				Expect(info).ToNot(BeNil())
				Expect(info.Path).To(Equal("/root1.txt"))

				info = res.Infos[1]
				Expect(info).ToNot(BeNil())
				Expect(info.Path).To(Equal("/foo"))
			})
		})
	})
})
