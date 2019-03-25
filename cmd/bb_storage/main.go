package main

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/ac"
	blobstore "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/configuration"
	"github.com/buildbarn/bb-storage/pkg/opencensus"
	"github.com/buildbarn/bb-storage/pkg/util"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opencensus.io/plugin/ocgrpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb-storage bb-storage.conf")
	}

	storageConfiguration, err := configuration.GetStorageConfiguration(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}

	util.ValidateTLS(storageConfiguration.Tls)

	// Web server for metrics and profiling.
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(util.HTTPListenAndServe(&storageConfiguration.MetricsListenAddress,
			storageConfiguration.Tls, nil))
	}()

	if storageConfiguration.Jaeger != nil {
		opencensus.Initialize(storageConfiguration.Jaeger)
	}

	// Storage access.
	contentAddressableStorageBlobAccess, actionCacheBlobAccess, err := blobstore.CreateBlobAccessObjectsFromConfig(storageConfiguration.Blobstore)
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}
	actionCache := ac.NewBlobAccessActionCache(actionCacheBlobAccess)

	// Let GetCapabilities() work, even for instances that don't
	// have a scheduler attached to them, but do allow uploading
	// results into the Action Cache.
	schedulers := map[string]builder.BuildQueue{}
	allowActionCacheUpdatesForInstances := map[string]bool{}
	if len(storageConfiguration.AllowAcUpdatesForInstances) > 0 {
		fallback := builder.NewNonExecutableBuildQueue()
		for _, instance := range storageConfiguration.AllowAcUpdatesForInstances {
			schedulers[instance] = fallback
			allowActionCacheUpdatesForInstances[instance] = true
		}
	}

	// Backends capable of compiling.
	for name, endpoint := range storageConfiguration.Schedulers {
		scheduler, err := grpc.Dial(
			endpoint,
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
			grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
		if err != nil {
			log.Fatal("Failed to create scheduler RPC client: ", err)
		}
		schedulers[name] = builder.NewForwardingBuildQueue(scheduler)
	}
	buildQueue := builder.NewDemultiplexingBuildQueue(func(instance string) (builder.BuildQueue, error) {
		scheduler, ok := schedulers[instance]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Unknown instance name")
		}
		return scheduler, nil
	})

	// RPC server.
	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	}
	creds, err := util.MakeGRPCCreds(storageConfiguration.Tls)
	if err != nil {
		log.Fatal("Loading TLS materials failed: ", err)
	} else if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	s := grpc.NewServer(opts...)
	remoteexecution.RegisterActionCacheServer(s, ac.NewActionCacheServer(actionCache, allowActionCacheUpdatesForInstances))
	remoteexecution.RegisterContentAddressableStorageServer(s, cas.NewContentAddressableStorageServer(contentAddressableStorageBlobAccess))
	bytestream.RegisterByteStreamServer(s, cas.NewByteStreamServer(contentAddressableStorageBlobAccess, 1<<16))
	remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
	remoteexecution.RegisterExecutionServer(s, buildQueue)
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.Register(s)

	sock, err := net.Listen("tcp", storageConfiguration.GrpcListenAddress)
	if err != nil {
		log.Fatal("Failed to create listening socket: ", err)
	}
	if err := s.Serve(sock); err != nil {
		log.Fatal("Failed to serve RPC server: ", err)
	}
}
