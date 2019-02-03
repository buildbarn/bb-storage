package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/ac"
	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

func main() {
	var (
		blobstoreConfig  = flag.String("blobstore-config", "/config/blobstore.conf", "Configuration for blob storage")
		webListenAddress = flag.String("web.listen-address", ":80", "Port on which to expose metrics")
	)
	flag.Parse()

	// Web server for metrics and profiling.
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(*webListenAddress, nil))
	}()

	// Storage access.
	contentAddressableStorageBlobAccess, actionCacheBlobAccess, err := configuration.CreateBlobAccessObjectsFromConfig(*blobstoreConfig)
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}
	actionCache := ac.NewBlobAccessActionCache(actionCacheBlobAccess)

	// RPC server.
	s := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	remoteexecution.RegisterActionCacheServer(s, ac.NewActionCacheServer(actionCache, true))
	remoteexecution.RegisterContentAddressableStorageServer(s, cas.NewContentAddressableStorageServer(contentAddressableStorageBlobAccess))
	bytestream.RegisterByteStreamServer(s, cas.NewByteStreamServer(contentAddressableStorageBlobAccess, 1<<16))
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.Register(s)

	sock, err := net.Listen("tcp", ":8982")
	if err != nil {
		log.Fatal("Failed to create listening socket: ", err)
	}
	if err := s.Serve(sock); err != nil {
		log.Fatal("Failed to serve RPC server: ", err)
	}
}
