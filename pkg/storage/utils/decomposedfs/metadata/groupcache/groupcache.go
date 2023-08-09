package groupcache

import (
	"sync"
	"time"

	"github.com/cs3org/reva/v2/pkg/registry"
	httpServer "github.com/go-micro/plugins/v4/server/http"
	gc "github.com/mailgun/groupcache/v2"
	"github.com/rs/zerolog/log"
	"go-micro.dev/v4"
	mregistry "go-micro.dev/v4/registry"
	"go-micro.dev/v4/server"
	"go-micro.dev/v4/util/addr"
)

var mu = sync.Mutex{}
var cache *Cache

type Cache struct {
}

func New(name string) (*Cache, error) {
	r := registry.GetRegistry()

	mu.Lock()
	defer mu.Unlock()

	if cache != nil {
		return cache, nil
	}

	ownAddress, err := addr.Extract("0.0.0.0")
	if err != nil {
		return nil, err
	}
	service_name := "com.owncloud.groupcache-" + name

	srv := httpServer.NewServer(
		// server.Name("groupcache-store"),
		server.Name(service_name),
	)
	pool := gc.NewHTTPPoolOpts(ownAddress, &gc.HTTPPoolOptions{})
	hd := srv.NewHandler(pool)
	if err := srv.Handle(hd); err != nil {
		return nil, err
	}

	// Watch for changes and update pool
	watcher, err := r.Watch(func(opts *mregistry.WatchOptions) { opts.Service = service_name })
	if err != nil {
		return nil, err
	}
	// nodes := map[string]struct{}{}
	go func() {
		for {
			result, err := watcher.Next()
			if err != nil {
				continue
			}
			// switch result.Action {
			// case "delete":
			// 	for _, node := range result.Service.Nodes {
			// 		delete(nodes, "http://"+node.Address)
			// 	}
			// case "create", "update":
			// 	for _, node := range result.Service.Nodes {
			// 		nodes["http://"+node.Address] = struct{}{}
			// 	}
			// }
			// nodeList := []string{}
			// for node, _ := range nodes {
			// 	nodeList = append(nodeList, node)
			// }
			// log.Info().Interface("Service", result.Service).Str("action", result.Action).Interface("nodes", nodeList).Msg("Setting groupcache nodes")
			// pool.Set(nodeList...)

			services, err := r.GetService(service_name)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get the list of service nodes")
				continue
			}
			nodes := []string{}
			for _, node := range services[0].Nodes {
				nodes = append(nodes, "http://"+node.Address)
			}
			pool.Set(nodes...)
			log.Info().Interface("Service", result.Service).Str("action", result.Action).Interface("nodes", nodes).Msg("Setting groupcache nodes")
		}
	}()

	//Start service
	service := micro.NewService(
		micro.Name(service_name),
		micro.Server(srv),
		micro.Address("0.0.0.0:8080"),
		micro.Registry(r),
		micro.RegisterTTL(time.Second*30),
		micro.RegisterInterval(time.Second*10),
	)
	service.Init()

	addr := srv.Options().Address
	log.Info().Str("ownAddress", ownAddress).Str("addr", addr).Msg("groupcache server address")
	_ = addr

	go service.Run()

	// // Initialize pool
	// services, err := r.GetService(service_name)
	// if err != nil {
	// 	return nil, err
	// }
	// nodes := []string{}
	// for _, node := range services[0].Nodes {
	// 	nodes = append(nodes, node.Address)
	// }
	// pool.Set(nodes...)

	cache = &Cache{}
	return cache, nil
}
