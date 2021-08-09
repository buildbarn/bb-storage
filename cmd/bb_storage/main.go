package main

import (
	"log"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_storage bb_storage.jsonnet")
	}
	var configuration bb_storage.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	// Storage access.
	contentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
		configuration.Blobstore,
		grpcClientFactory,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal(err)
	}

	contentAddressableStorage, err = newScannableAuthorizingBlobAccess(contentAddressableStorage, configuration.ContentAddressableStorageAuthorizers)
	if err != nil {
		log.Fatalf("Failed to create Content Addressable Storage authorizers: %v", err)
	}

	actionCache, acPutAuthorizer, err := newNonScannableAuthorizingBlobAccess(actionCache, configuration.ActionCacheAuthorizers)
	if err != nil {
		log.Fatalf("Failed to create Action Cache authorizers: %v", err)
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
		indirectContentAddressableStorage, err = newScannableAuthorizingBlobAccess(info.BlobAccess, configuration.IndirectContentAddressableStorageAuthorizers)
		if err != nil {
			log.Fatal("Failed to create Indirect Content Addressable Storage authorizer: ", err)
		}
	}

	// Buildbarn extension: Initial Size Class Cache (ISCC).
	var initialSizeClassCache blobstore.BlobAccess
	if configuration.InitialSizeClassCache != nil {
		info, err := blobstore_configuration.NewBlobAccessFromConfiguration(
			configuration.InitialSizeClassCache,
			blobstore_configuration.NewISCCBlobAccessCreator(
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Initial Size Class Cache: ", err)
		}
		initialSizeClassCache, _, err = newNonScannableAuthorizingBlobAccess(info.BlobAccess, configuration.InitialSizeClassCacheAuthorizers)
		if err != nil {
			log.Fatal("Failed to create Initial Size Class Cache authorizer: ", err)
		}
	}

	// Create a demultiplexing build queue that forwards traffic to
	// one or more schedulers specified in the configuration file.
	buildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(
		configuration.Schedulers,
		grpcClientFactory,
		acPutAuthorizer)
	if err != nil {
		log.Fatal(err)
	}

	buildQueue = builder.NewUpdateEnabledTogglingBuildQueue(buildQueue, acPutAuthorizer)

	go func() {
		log.Fatal(
			"gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
				configuration.GrpcServers,
				func(s grpc.ServiceRegistrar) {
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
					if initialSizeClassCache != nil {
						iscc.RegisterInitialSizeClassCacheServer(
							s,
							grpcservers.NewInitialSizeClassCacheServer(
								initialSizeClassCache,
								int(configuration.MaximumMessageSizeBytes)))
					}
					remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
					remoteexecution.RegisterExecutionServer(s, buildQueue)
				}))
	}()

	lifecycleState.MarkReadyAndWait()
}

func newNonScannableAuthorizingBlobAccess(base blobstore.BlobAccess, configuration *bb_storage.NonScannableAuthorizersConfiguration) (blobstore.BlobAccess, auth.Authorizer, error) {
	getAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetGet())
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Get() authorizer")
	}

	putAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetPut())
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Put() authorizer")
	}

	return blobstore.NewAuthorizingBlobAccess(base, getAuthorizer, putAuthorizer, nil), putAuthorizer, nil
}

func newScannableAuthorizingBlobAccess(base blobstore.BlobAccess, configuration *bb_storage.ScannableAuthorizersConfiguration) (blobstore.BlobAccess, error) {
	getAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetGet())
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create Get() authorizer")
	}

	putAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetPut())
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create Put() authorizer")
	}

	findMissingAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetFindMissing())
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create FindMissing() authorizer")
	}

	return blobstore.NewAuthorizingBlobAccess(base, getAuthorizer, putAuthorizer, findMissingAuthorizer), nil
}
