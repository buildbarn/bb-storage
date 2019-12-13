package main

import (
	"log"
	"net/http"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/ac"
	"github.com/buildbarn/bb-storage/pkg/blobstore/completenesschecking"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/cas"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/opencensus"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/gorilla/mux"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_storage bb_storage.jsonnet")
	}
	var configuration bb_storage.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}

	if configuration.Jaeger != nil {
		opencensus.Initialize(configuration.Jaeger)
	}

	// Storage access.
	contentAddressableStorageBlobAccess, actionCache, err := blobstore_configuration.CreateBlobAccessObjectsFromConfig(
		configuration.Blobstore,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}

	// If this instance of bb-storage has access to all data (as in,
	// it's not a single shard within a distributed setup), it can
	// be configured to verify that all objects referenced by
	// ActionResults are present in the Content Addressable Storage.
	// Such validation is required by Bazel.
	if configuration.VerifyActionResultCompleteness {
		actionCache = completenesschecking.NewCompletenessCheckingBlobAccess(
			actionCache,
			cas.NewBlobAccessContentAddressableStorage(
				contentAddressableStorageBlobAccess,
				int(configuration.MaximumMessageSizeBytes)),
			contentAddressableStorageBlobAccess,
			100,
			int(configuration.MaximumMessageSizeBytes))
	}

	// Let GetCapabilities() work, even for instances that don't
	// have a scheduler attached to them, but do allow uploading
	// results into the Action Cache.
	schedulers := map[string]builder.BuildQueue{}
	allowActionCacheUpdatesForInstances := map[string]bool{}
	if len(configuration.AllowAcUpdatesForInstances) > 0 {
		fallback := builder.NewNonExecutableBuildQueue()
		for _, instance := range configuration.AllowAcUpdatesForInstances {
			schedulers[instance] = fallback
			allowActionCacheUpdatesForInstances[instance] = true
		}
	}

	// Backends capable of compiling.
	for name, endpoint := range configuration.Schedulers {
		scheduler, err := bb_grpc.NewGRPCClientFromConfiguration(endpoint)
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

	go func() {
		log.Fatal(
			"gRPC server failure: ",
			bb_grpc.NewGRPCServersFromConfigurationAndServe(
				configuration.GrpcServers,
				func(s *grpc.Server) {
					remoteexecution.RegisterActionCacheServer(s, ac.NewActionCacheServer(actionCache, allowActionCacheUpdatesForInstances, int(configuration.MaximumMessageSizeBytes)))
					remoteexecution.RegisterContentAddressableStorageServer(s, cas.NewContentAddressableStorageServer(contentAddressableStorageBlobAccess))
					bytestream.RegisterByteStreamServer(s, cas.NewByteStreamServer(contentAddressableStorageBlobAccess, 1<<16))
					remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
					remoteexecution.RegisterExecutionServer(s, buildQueue)
				}))
	}()

	// Web server for metrics and profiling.
	router := mux.NewRouter()
	util.RegisterAdministrativeHTTPEndpoints(router)
	log.Fatal(http.ListenAndServe(configuration.HttpListenAddress, router))
}
