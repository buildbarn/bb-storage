package main

import (
	"context"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	auth_configuration "github.com/buildbarn/bb-storage/pkg/auth/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_storage bb_storage.jsonnet")
		}
		var configuration bb_storage.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global, dependenciesGroup)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

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
				dependenciesGroup,
				configuration.ContentAddressableStorage,
				blobstore_configuration.NewCASBlobAccessCreator(
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes)),
				grpcClientFactory)
			if err != nil {
				return util.StatusWrap(err, "Failed to create Content Addressable Storage")
			}
			cacheCapabilitiesProviders = append(
				cacheCapabilitiesProviders,
				info.BlobAccess,
				capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
					CacheCapabilities: &remoteexecution.CacheCapabilities{
						SupportedCompressors: configuration.SupportedCompressors,
					},
				}),
			)
			cacheCapabilitiesAuthorizers = append(cacheCapabilitiesAuthorizers, allAuthorizers...)
			contentAddressableStorageInfo = &info
			contentAddressableStorage = authorizedBackend
		}

		// Action Cache (AC).
		var actionCache blobstore.BlobAccess
		if configuration.ActionCache != nil {
			info, authorizedBackend, allAuthorizers, putAuthorizer, err := newNonScannableBlobAccess(
				dependenciesGroup,
				configuration.ActionCache,
				blobstore_configuration.NewACBlobAccessCreator(
					contentAddressableStorageInfo,
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes),
				),
				grpcClientFactory)
			if err != nil {
				return util.StatusWrap(err, "Failed to create Action Cache")
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
				dependenciesGroup,
				configuration.IndirectContentAddressableStorage,
				blobstore_configuration.NewICASBlobAccessCreator(
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes)),
				grpcClientFactory)
			if err != nil {
				return util.StatusWrap(err, "Failed to create Indirect Content Addressable Storage")
			}
			indirectContentAddressableStorage = authorizedBackend
		}

		// Buildbarn extension: Initial Size Class Cache (ISCC).
		var initialSizeClassCache blobstore.BlobAccess
		if configuration.InitialSizeClassCache != nil {
			_, authorizedBackend, _, _, err := newNonScannableBlobAccess(
				dependenciesGroup,
				configuration.InitialSizeClassCache,
				blobstore_configuration.NewISCCBlobAccessCreator(
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes)),
				grpcClientFactory)
			if err != nil {
				return util.StatusWrap(err, "Failed to create Initial Size Class Cache")
			}
			initialSizeClassCache = authorizedBackend
		}

		// Buildbarn extension: File System Access Cache (FSAC).
		var fileSystemAccessCache blobstore.BlobAccess
		if configuration.FileSystemAccessCache != nil {
			_, authorizedBackend, _, _, err := newNonScannableBlobAccess(
				dependenciesGroup,
				configuration.FileSystemAccessCache,
				blobstore_configuration.NewFSACBlobAccessCreator(
					grpcClientFactory,
					int(configuration.MaximumMessageSizeBytes)),
				grpcClientFactory)
			if err != nil {
				return util.StatusWrap(err, "Failed to create File System Access Cache")
			}
			fileSystemAccessCache = authorizedBackend
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
			baseBuildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(configuration.Schedulers, dependenciesGroup, grpcClientFactory)
			if err != nil {
				return err
			}
			executeAuthorizer, err := auth_configuration.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetExecuteAuthorizer(), dependenciesGroup, grpcClientFactory)
			if err != nil {
				return util.StatusWrap(err, "Failed to create execute authorizer")
			}
			buildQueue = builder.NewAuthorizingBuildQueue(baseBuildQueue, executeAuthorizer)
			capabilitiesProviders = append(capabilitiesProviders, buildQueue)
		}

		if err := bb_grpc.NewServersFromConfigurationAndServe(
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
				if fileSystemAccessCache != nil {
					fsac.RegisterFileSystemAccessCacheServer(
						s,
						grpcservers.NewFileSystemAccessCacheServer(
							fileSystemAccessCache,
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
			},
			siblingsGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "gRPC server failure")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}

func newNonScannableBlobAccess(dependenciesGroup program.Group, configuration *bb_storage.NonScannableBlobAccessConfiguration, creator blobstore_configuration.BlobAccessCreator, grpcClientFactory bb_grpc.ClientFactory) (blobstore_configuration.BlobAccessInfo, blobstore.BlobAccess, []auth.Authorizer, auth.Authorizer, error) {
	info, err := blobstore_configuration.NewBlobAccessFromConfiguration(dependenciesGroup, configuration.Backend, creator)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, nil, err
	}

	getAuthorizer, err := auth_configuration.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetAuthorizer, dependenciesGroup, grpcClientFactory)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, nil, util.StatusWrap(err, "Failed to create Get() authorizer")
	}
	putAuthorizer, err := auth_configuration.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.PutAuthorizer, dependenciesGroup, grpcClientFactory)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, nil, util.StatusWrap(err, "Failed to create Put() authorizer")
	}

	return info,
		blobstore.NewAuthorizingBlobAccess(info.BlobAccess, getAuthorizer, putAuthorizer, nil),
		[]auth.Authorizer{getAuthorizer, putAuthorizer},
		putAuthorizer,
		nil
}

func newScannableBlobAccess(dependenciesGroup program.Group, configuration *bb_storage.ScannableBlobAccessConfiguration, creator blobstore_configuration.BlobAccessCreator, grpcClientFactory bb_grpc.ClientFactory) (blobstore_configuration.BlobAccessInfo, blobstore.BlobAccess, []auth.Authorizer, error) {
	info, err := blobstore_configuration.NewBlobAccessFromConfiguration(dependenciesGroup, configuration.Backend, creator)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, err
	}

	getAuthorizer, err := auth_configuration.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.GetAuthorizer, dependenciesGroup, grpcClientFactory)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, util.StatusWrap(err, "Failed to create Get() authorizer")
	}
	putAuthorizer, err := auth_configuration.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.PutAuthorizer, dependenciesGroup, grpcClientFactory)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, util.StatusWrap(err, "Failed to create Put() authorizer")
	}
	findMissingAuthorizer, err := auth_configuration.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(configuration.FindMissingAuthorizer, dependenciesGroup, grpcClientFactory)
	if err != nil {
		return blobstore_configuration.BlobAccessInfo{}, nil, nil, util.StatusWrap(err, "Failed to create FindMissing() authorizer")
	}

	return info,
		blobstore.NewAuthorizingBlobAccess(info.BlobAccess, getAuthorizer, putAuthorizer, findMissingAuthorizer),
		[]auth.Authorizer{getAuthorizer, putAuthorizer, findMissingAuthorizer},
		nil
}
