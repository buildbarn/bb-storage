package main

import (
	"log"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/storage"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_storage bb_storage.jsonnet")
	}
	var configuration storage.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	lifecycleState, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	// Storage access.
	contentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
		configuration.Blobstore,
		bb_grpc.DefaultClientFactory,
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
				bb_grpc.DefaultClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Indirect Content Addressable Storage: ", err)
		}
		indirectContentAddressableStorage = info.BlobAccess
	}

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
	}

	// Create a demultiplexing build queue that forwards traffic to
	// one or more schedulers specified in the configuration file.
	buildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(
		configuration.Schedulers,
		bb_grpc.DefaultClientFactory,
		allowActionCacheUpdatesTrie.Contains)
	if err != nil {
		log.Fatal(err)
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

	lifecycleState.MarkReadyAndWait()
}
