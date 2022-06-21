package main

import (
	"context"
	"log"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
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
	diagnosticsServer, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}
	signalContext := global.InstallTerminationSignalHandler()
	terminationGroup, terminationContext := errgroup.WithContext(signalContext)
	global.ServeDiagnostics(terminationContext, terminationGroup, diagnosticsServer)

	// Providers for data returned by ServerCapabilities.cache_capabilities
	// as part of the GetCapabilities() call. We permit these calls
	// if the client is permitted to at least one method against one
	// of the data stores described in REv2.
	var cacheCapabilitiesProviders []capabilities.Provider
	var cacheCapabilitiesAuthorizers []auth.Authorizer

	// Content Addressable Storage (CAS).
	var contentAddressableStorageInfo *blobstore_configuration.BlobAccessInfo
	var contentAddressableStorage blobstore.BlobAccess
	if configuration.ContentAddressableStorage != nil {
		info, authorizedBackend, allAuthorizers, err := newScannableBlobAccess(
			terminationContext,
			terminationGroup,
			configuration.ContentAddressableStorage,
			blobstore_configuration.NewCASBlobAccessCreator(
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Content Addressable Storage: ", err)
		}
		cacheCapabilitiesProviders = append(cacheCapabilitiesProviders, info.BlobAccess)
		cacheCapabilitiesAuthorizers = append(cacheCapabilitiesAuthorizers, allAuthorizers...)
		contentAddressableStorageInfo = &info
		contentAddressableStorage = authorizedBackend
	}

	// Action Cache (AC).
	var actionCache blobstore.BlobAccess
	if configuration.ActionCache != nil {
		info, authorizedBackend, allAuthorizers, putAuthorizer, err := newNonScannableBlobAccess(
			terminationContext,
			terminationGroup,
			configuration.ActionCache,
			blobstore_configuration.NewACBlobAccessCreator(
				contentAddressableStorageInfo,
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Action Cache: ", err)
		}
		cacheCapabilitiesProviders = append(
			cacheCapabilitiesProviders,
			capabilities.NewActionCacheUpdateEnabledClearingProvider(info.BlobAccess, putAuthorizer))
		cacheCapabilitiesAuthorizers = append(cacheCapabilitiesAuthorizers, allAuthorizers...)
		actionCache = authorizedBackend
	}

	// Buildbarn extension: Indirect Content Addressable Storage (ICAS).
	var indirectContentAddressableStorage blobstore.BlobAccess
	if configuration.IndirectContentAddressableStorage != nil {
		_, authorizedBackend, _, err := newScannableBlobAccess(
			terminationContext,
			terminationGroup,
			configuration.IndirectContentAddressableStorage,
			blobstore_configuration.NewICASBlobAccessCreator(
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Indirect Content Addressable Storage: ", err)
		}
		indirectContentAddressableStorage = authorizedBackend
	}

	// Buildbarn extension: Initial Size Class Cache (ISCC).
	var initialSizeClassCache blobstore.BlobAccess
	if configuration.InitialSizeClassCache != nil {
		_, authorizedBackend, _, _, err := newNonScannableBlobAccess(
			terminationContext,
			terminationGroup,
			configuration.InitialSizeClassCache,
			blobstore_configuration.NewISCCBlobAccessCreator(
				grpcClientFactory,
				int(configuration.MaximumMessageSizeBytes)))
		if err != nil {
			log.Fatal("Failed to create Initial Size Class Cache: ", err)
		}
		initialSizeClassCache = authorizedBackend
	}

	var capabilitiesProviders []capabilities.Provider
	if len(cacheCapabilitiesProviders) > 0 {
		capabilitiesProviders = append(
			capabilitiesProviders,
			capabilities.NewAuthorizingProvider(
				capabilities.NewMergingProvider(cacheCapabilitiesProviders),
				auth.NewAnyAuthorizer(cacheCapabilitiesAuthorizers)))
	}

	// Create a demultiplexing build queue that forwards traffic to
	// one or more schedulers specified in the configuration file.
	var buildQueue builder.BuildQueue
	if len(configuration.Schedulers) > 0 {
		baseBuildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(configuration.Schedulers, grpcClientFactory)
		if err != nil {
			log.Fatal(err)
		}
		executeAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetExecuteAuthorizer())
		if err != nil {
			log.Fatal("Failed to create execute authorizer: ", err)
		}
		buildQueue = builder.NewAuthorizingBuildQueue(baseBuildQueue, executeAuthorizer)
		capabilitiesProviders = append(capabilitiesProviders, buildQueue)
	}

	grpcServers, err := bb_grpc.NewServersFromConfigurationAndServe(
		terminationContext,
		terminationGroup,
		configuration.GrpcServers,
		func(s grpc.ServiceRegistrar) {
			if contentAddressableStorage != nil {
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
			}
			if actionCache != nil {
				remoteexecution.RegisterActionCacheServer(
					s,
					grpcservers.NewActionCacheServer(
						actionCache,
						int(configuration.MaximumMessageSizeBytes)))
			}
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
			if buildQueue != nil {
				remoteexecution.RegisterExecutionServer(s, buildQueue)
			}
			if len(capabilitiesProviders) > 0 {
				remoteexecution.RegisterCapabilitiesServer(
					s,
					capabilities.NewServer(
						capabilities.NewMergingProvider(capabilitiesProviders)))
			}
		})
	if err != nil {
		log.Fatal("gRPC server failure: ", err)
	}

	diagnosticsServer.SetReady()
	grpcServers.SetReady()
	terminationGroup.Wait()
}

func newNonScannableBlobAccess(terminationContext context.Context, terminationGroup *errgroup.Group, configuration *bb_storage.NonScannableBlobAccessConfiguration, creator blobstore_configuration.BlobAccessCreator) (blobstore_configuration.BlobAccessInfo, blobstore.BlobAccess, []auth.Authorizer, auth.Authorizer, error) {
	info, err := blobstore_configuration.NewBlobAccessFromConfiguration(terminationContext, terminationGroup, configuration.Backend, creator)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, nil, err
	}

	getAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetAuthorizer)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, nil, util.StatusWrap(err, "Failed to create Get() authorizer")
	}
	putAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.PutAuthorizer)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, nil, util.StatusWrap(err, "Failed to create Put() authorizer")
	}

	return info,
		blobstore.NewAuthorizingBlobAccess(info.BlobAccess, getAuthorizer, putAuthorizer, nil),
		[]auth.Authorizer{getAuthorizer, putAuthorizer},
		putAuthorizer,
		nil
}

func newScannableBlobAccess(terminationContext context.Context, terminationGroup *errgroup.Group, configuration *bb_storage.ScannableBlobAccessConfiguration, creator blobstore_configuration.BlobAccessCreator) (blobstore_configuration.BlobAccessInfo, blobstore.BlobAccess, []auth.Authorizer, error) {
	info, err := blobstore_configuration.NewBlobAccessFromConfiguration(terminationContext, terminationGroup, configuration.Backend, creator)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, err
	}

	getAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetAuthorizer)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, util.StatusWrap(err, "Failed to create Get() authorizer")
	}
	putAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.PutAuthorizer)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, util.StatusWrap(err, "Failed to create Put() authorizer")
	}
	findMissingAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.FindMissingAuthorizer)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, util.StatusWrap(err, "Failed to create FindMissing() authorizer")
	}

	return info,
		blobstore.NewAuthorizingBlobAccess(info.BlobAccess, getAuthorizer, putAuthorizer, findMissingAuthorizer),
		[]auth.Authorizer{getAuthorizer, putAuthorizer, findMissingAuthorizer},
		nil
}
