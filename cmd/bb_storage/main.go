package main

import (
	"log"
	"net/http"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
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
	if err := global.ApplyConfiguration(configuration.Global); err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	// Storage access.
	grpcClientFactory := bb_grpc.NewDeduplicatingClientFactory(bb_grpc.BaseClientFactory)
	contentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
		configuration.Blobstore,
		grpcClientFactory,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal(err)
	}

	// Buildbarn extension: Indirect Content Addressable Storage
	// (ICAS) access.
	var indirectContentAddressableStorage blobstore.BlobAccess
	if configuration.IndirectContentAddressableStorage != nil {
		info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
			configuration.IndirectContentAddressableStorage,
			blobstore_configuration.NewICASBlobAccessCreator(
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Indirect Content Addressable Storage: ", err)
		}
		indirectContentAddressableStorage = info.BlobAccess
	}

	// Create a trie that maps instance names to schedulers capable
	// of picking up build actions.
	buildQueuesTrie := digest.NewInstanceNameTrie()
	type buildQueueInfo struct {
		backend             builder.BuildQueue
		backendName         digest.InstanceName
		instanceNamePatcher digest.InstanceNamePatcher
	}
	var buildQueues []buildQueueInfo
	for k, scheduler := range configuration.Schedulers {
		matchInstanceNamePrefix, err := digest.NewInstanceName(k)
		if err != nil {
			log.Fatalf("Invalid instance name %#v: %s", k, err)
		}
		addInstanceNamePrefix, err := digest.NewInstanceName(scheduler.AddInstanceNamePrefix)
		if err != nil {
			log.Fatalf("Invalid instance name %#v: %s", scheduler.AddInstanceNamePrefix, err)
		}
		endpoint, err := grpcClientFactory.NewClientFromConfiguration(scheduler.Endpoint)
		if err != nil {
			log.Fatal("Failed to create scheduler RPC client: ", err)
		}
		buildQueuesTrie.Set(matchInstanceNamePrefix, len(buildQueues))
		buildQueues = append(buildQueues, buildQueueInfo{
			backend:     builder.NewForwardingBuildQueue(endpoint),
			backendName: matchInstanceNamePrefix,
			instanceNamePatcher: digest.NewInstanceNamePatcher(
				matchInstanceNamePrefix,
				addInstanceNamePrefix),
		})
	}
	buildQueue := builder.NewDemultiplexingBuildQueue(func(instanceName digest.InstanceName) (builder.BuildQueue, digest.InstanceName, digest.InstanceName, error) {
		idx := buildQueuesTrie.Get(instanceName)
		if idx < 0 {
			return nil, digest.EmptyInstanceName, digest.EmptyInstanceName, status.Errorf(codes.InvalidArgument, "Unknown instance name")
		}
		return buildQueues[idx].backend, buildQueues[idx].backendName, buildQueues[idx].instanceNamePatcher.PatchInstanceName(instanceName), nil
	})

	// Create a trie for which instance names provide a writable
	// Action Cache. Use that trie to both limit BlobAccess writes
	// and determine the value of UpdateEnabled in GetCapabilities()
	// results.
	allowActionCacheUpdatesTrie := digest.NewInstanceNameTrie()
	for _, k := range configuration.AllowAcUpdatesForInstanceNamePrefixes {
		instanceNamePrefix, err := digest.NewInstanceName(k)
		if err != nil {
			log.Fatalf("Invalid instance name %#v: %s", k, err)
		}
		allowActionCacheUpdatesTrie.Set(instanceNamePrefix, 0)

		// Ensure that instance names for which we don't have a
		// scheduler, but allow AC updates, at least have the
		// NonExecutableBuildQueue. This makes GetCapabilities()
		// work for those instance names.
		if !buildQueuesTrie.Contains(instanceNamePrefix) {
			buildQueuesTrie.Set(instanceNamePrefix, 0)
			buildQueuesTrie.Set(instanceNamePrefix, len(buildQueues))
			buildQueues = append(buildQueues, buildQueueInfo{
				backend:             builder.NonExecutableBuildQueue,
				backendName:         instanceNamePrefix,
				instanceNamePatcher: digest.NoopInstanceNamePatcher,
			})
		}
	}
	actionCache = blobstore.NewInstanceNameAccessCheckingBlobAccess(
		actionCache,
		allowActionCacheUpdatesTrie.Contains)
	buildQueue = builder.NewUpdateEnabledTogglingBuildQueue(
		buildQueue,
		allowActionCacheUpdatesTrie.Contains)

	go func() {
		log.Fatal(
			"gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
				configuration.GrpcServers,
				func(s *grpc.Server) {
					remoteexecution.RegisterActionCacheServer(
						s,
						grpcservers.NewActionCacheServer(
							actionCache,
							int(configuration.MaximumMessageSizeBytes)))
					remoteexecution.RegisterContentAddressableStorageServer(
						s,
						grpcservers.NewContentAddressableStorageServer(
							contentAddressableStorage,
							configuration.MaximumMessageSizeBytes))
					bytestream.RegisterByteStreamServer(
						s,
						grpcservers.NewByteStreamServer(
							contentAddressableStorage,
							1<<16))
					if indirectContentAddressableStorage != nil {
						icas.RegisterIndirectContentAddressableStorageServer(
							s,
							grpcservers.NewIndirectContentAddressableStorageServer(
								indirectContentAddressableStorage,
								int(configuration.MaximumMessageSizeBytes)))

					}
					remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
					remoteexecution.RegisterExecutionServer(s, buildQueue)
				}))
	}()

	// Web server for metrics and profiling.
	router := mux.NewRouter()
	util.RegisterAdministrativeHTTPEndpoints(router)
	log.Fatal(http.ListenAndServe(configuration.HttpListenAddress, router))
}
