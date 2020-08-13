package configuration

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/circular"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cloud"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readcaching"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readfallback"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/ptypes"

	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"

	// Although not explicitly used here, we want to support a file blob
	// backend for debug.
	_ "gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/gcsblob"

	// Same thing for in-memory blob storage.
	_ "gocloud.dev/blob/memblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"

	"golang.org/x/oauth2/google"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/storage"
)

// BlobAccessInfo contains an instance of BlobAccess and information
// relevant to its creation. It is returned by functions that construct
// BlobAccess instances, such as NewBlobAccessFromConfiguration().
type BlobAccessInfo struct {
	BlobAccess      blobstore.BlobAccess
	DigestKeyFormat digest.KeyFormat
}

func newNestedBlobAccessBare(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (BlobAccessInfo, string, error) {
	readBufferFactory := creator.GetReadBufferFactory()
	storageTypeName := creator.GetStorageTypeName()
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Circular:
		implementation, err := createCircularBlobAccess(backend.Circular, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      implementation,
			DigestKeyFormat: creator.GetBaseDigestKeyFormat(),
		}, "circular", nil
	case *pb.BlobAccessConfiguration_Cloud:
		digestKeyFormat := creator.GetBaseDigestKeyFormat()
		switch backendConfig := backend.Cloud.Config.(type) {
		case *pb.CloudBlobAccessConfiguration_Url:
			ctx := context.Background()
			bucket, err := blob.OpenBucket(ctx, backendConfig.Url)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			return BlobAccessInfo{
				BlobAccess:      cloud.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, readBufferFactory, digestKeyFormat, nil),
				DigestKeyFormat: digestKeyFormat,
			}, "cloud", nil
		case *pb.CloudBlobAccessConfiguration_Azure:
			credential, err := azureblob.NewCredential(azureblob.AccountName(backendConfig.Azure.AccountName), azureblob.AccountKey(backendConfig.Azure.AccountKey))
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			pipeline := azureblob.NewPipeline(credential, azblob.PipelineOptions{})
			ctx := context.Background()
			bucket, err := azureblob.OpenBucket(ctx, pipeline, azureblob.AccountName(backendConfig.Azure.AccountName), backendConfig.Azure.ContainerName, nil)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			return BlobAccessInfo{
				BlobAccess:      cloud.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, readBufferFactory, digestKeyFormat, nil),
				DigestKeyFormat: digestKeyFormat,
			}, "azure", nil
		case *pb.CloudBlobAccessConfiguration_Gcs:
			var creds *google.Credentials
			var err error
			ctx := context.Background()
			if backendConfig.Gcs.Credentials != "" {
				creds, err = google.CredentialsFromJSON(ctx, []byte(backendConfig.Gcs.Credentials), storage.ScopeReadWrite)
			} else {
				creds, err = google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
			}
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			bucket, err := gcsblob.OpenBucket(ctx, client, backendConfig.Gcs.Bucket, nil)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			return BlobAccessInfo{
				BlobAccess:      cloud.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, readBufferFactory, digestKeyFormat, nil),
				DigestKeyFormat: digestKeyFormat,
			}, "gcs", nil
		case *pb.CloudBlobAccessConfiguration_S3:
			sess, err := aws.NewSessionFromConfiguration(backendConfig.S3.AwsSession)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create AWS session")
			}
			ctx := context.Background()
			bucket, err := s3blob.OpenBucket(ctx, sess, backendConfig.S3.Bucket, nil)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			minRefreshAge, err := ptypes.Duration(backendConfig.S3.MinimumRefreshAge)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain S3 refresh age")
			}
			return BlobAccessInfo{
				BlobAccess:      cloud.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, readBufferFactory, digestKeyFormat, cloud.NewS3CopyMutator(minRefreshAge, clock.SystemClock)),
				DigestKeyFormat: digestKeyFormat,
			}, "s3", nil
		default:
			return BlobAccessInfo{}, "", status.Error(codes.InvalidArgument, "Cloud configuration did not contain a backend")
		}
	case *pb.BlobAccessConfiguration_Error:
		return BlobAccessInfo{
			BlobAccess:      blobstore.NewErrorBlobAccess(status.ErrorProto(backend.Error)),
			DigestKeyFormat: digest.KeyWithoutInstance,
		}, "error", nil
	case *pb.BlobAccessConfiguration_ReadCaching:
		slow, err := NewNestedBlobAccess(backend.ReadCaching.Slow, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		fast, err := NewNestedBlobAccess(backend.ReadCaching.Fast, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		replicator, err := NewBlobReplicatorFromConfiguration(backend.ReadCaching.Replicator, slow.BlobAccess, fast, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      readcaching.NewReadCachingBlobAccess(slow.BlobAccess, fast.BlobAccess, replicator),
			DigestKeyFormat: slow.DigestKeyFormat,
		}, "read_caching", nil
	case *pb.BlobAccessConfiguration_Redis:
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(backend.Redis.Tls)
		if err != nil {
			return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain TLS configuration")
		}

		var keyTTL time.Duration
		if backend.Redis.KeyTtl != nil {
			keyTTL, err = ptypes.Duration(backend.Redis.KeyTtl)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain key TTL configuration")
			}
		}

		var replicationTimeout time.Duration
		if backend.Redis.ReplicationTimeout != nil {
			replicationTimeout, err = ptypes.Duration(backend.Redis.ReplicationTimeout)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain replication timeout")
			}
		}

		var dialTimeout time.Duration
		if backend.Redis.DialTimeout != nil {
			dialTimeout, err = ptypes.Duration(backend.Redis.DialTimeout)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain dial timeout configuration")
			}
		}

		var readTimeout time.Duration
		if backend.Redis.ReadTimeout != nil {
			readTimeout, err = ptypes.Duration(backend.Redis.ReadTimeout)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain read timeout configuration")
			}
		}

		var writeTimeout time.Duration
		if backend.Redis.WriteTimeout != nil {
			writeTimeout, err = ptypes.Duration(backend.Redis.WriteTimeout)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain write timeout configuration")
			}
		}

		var redisClient blobstore.RedisClient
		switch mode := backend.Redis.Mode.(type) {
		case *pb.RedisBlobAccessConfiguration_Clustered:
			// Gather retry configuration (min/max delay and overall retry attempts)
			minRetryDur := time.Millisecond * 32
			if mode.Clustered.MinimumRetryBackoff != nil {
				minRetryDur, err = ptypes.Duration(mode.Clustered.MinimumRetryBackoff)
				if err != nil {
					return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain minimum retry back off configuration")
				}
			}

			maxRetryDur := time.Millisecond * 2048
			if mode.Clustered.MaximumRetryBackoff != nil {
				maxRetryDur, err = ptypes.Duration(mode.Clustered.MaximumRetryBackoff)
				if err != nil {
					return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain maximum retry back off")
				}
			}

			maxRetries := 16 // Default will be 16
			if mode.Clustered.MaximumRetries != 0 {
				maxRetries = int(mode.Clustered.MaximumRetries)
			}

			redisClient = redis.NewClusterClient(
				&redis.ClusterOptions{
					Addrs:           mode.Clustered.Endpoints,
					TLSConfig:       tlsConfig,
					ReadOnly:        true,
					MaxRetries:      maxRetries,
					MinRetryBackoff: minRetryDur,
					MaxRetryBackoff: maxRetryDur,
					DialTimeout:     dialTimeout,
					ReadTimeout:     readTimeout,
					WriteTimeout:    writeTimeout,
				})
		case *pb.RedisBlobAccessConfiguration_Single:
			redisClient = redis.NewClient(
				&redis.Options{
					Addr:         mode.Single.Endpoint,
					Password:     mode.Single.Password,
					DB:           int(mode.Single.Db),
					TLSConfig:    tlsConfig,
					DialTimeout:  dialTimeout,
					ReadTimeout:  readTimeout,
					WriteTimeout: writeTimeout,
				})
		default:
			return BlobAccessInfo{}, "", status.Errorf(codes.InvalidArgument, "Redis configuration must either be clustered or single server")
		}

		digestKeyFormat := creator.GetBaseDigestKeyFormat()
		return BlobAccessInfo{
			BlobAccess: blobstore.NewRedisBlobAccess(
				redisClient,
				readBufferFactory,
				digestKeyFormat,
				keyTTL,
				backend.Redis.ReplicationCount,
				replicationTimeout),
			DigestKeyFormat: digestKeyFormat,
		}, "redis", nil
	case *pb.BlobAccessConfiguration_Remote:
		return BlobAccessInfo{
			BlobAccess:      blobstore.NewRemoteBlobAccess(backend.Remote.Address, storageTypeName, readBufferFactory),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "remote", nil
	case *pb.BlobAccessConfiguration_Sharding:
		backends := make([]blobstore.BlobAccess, 0, len(backend.Sharding.Shards))
		weights := make([]uint32, 0, len(backend.Sharding.Shards))
		var combinedDigestKeyFormat *digest.KeyFormat
		for _, shard := range backend.Sharding.Shards {
			if shard.Backend == nil {
				// Drained backend.
				backends = append(backends, nil)
			} else {
				// Undrained backend.
				backend, err := NewNestedBlobAccess(shard.Backend, creator)
				if err != nil {
					return BlobAccessInfo{}, "", err
				}
				backends = append(backends, backend.BlobAccess)
				if combinedDigestKeyFormat == nil {
					combinedDigestKeyFormat = &backend.DigestKeyFormat
				} else {
					newDigestKeyFormat := combinedDigestKeyFormat.Combine(backend.DigestKeyFormat)
					combinedDigestKeyFormat = &newDigestKeyFormat
				}
			}

			if shard.Weight == 0 {
				return BlobAccessInfo{}, "", status.Errorf(codes.InvalidArgument, "Shards must have positive weights")
			}
			weights = append(weights, shard.Weight)
		}
		if combinedDigestKeyFormat == nil {
			return BlobAccessInfo{}, "", status.Errorf(codes.InvalidArgument, "Cannot create sharding blob access without any undrained backends")
		}
		return BlobAccessInfo{
			BlobAccess: sharding.NewShardingBlobAccess(
				backends,
				sharding.NewWeightedShardPermuter(weights),
				*combinedDigestKeyFormat,
				backend.Sharding.HashInitialization),
			DigestKeyFormat: *combinedDigestKeyFormat,
		}, "sharding", nil
	case *pb.BlobAccessConfiguration_SizeDistinguishing:
		small, err := NewNestedBlobAccess(backend.SizeDistinguishing.Small, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		large, err := NewNestedBlobAccess(backend.SizeDistinguishing.Large, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      blobstore.NewSizeDistinguishingBlobAccess(small.BlobAccess, large.BlobAccess, backend.SizeDistinguishing.CutoffSizeBytes),
			DigestKeyFormat: small.DigestKeyFormat.Combine(large.DigestKeyFormat),
		}, "size_distinguishing", nil
	case *pb.BlobAccessConfiguration_Mirrored:
		backendA, err := NewNestedBlobAccess(backend.Mirrored.BackendA, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		backendB, err := NewNestedBlobAccess(backend.Mirrored.BackendB, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		replicatorAToB, err := NewBlobReplicatorFromConfiguration(backend.Mirrored.ReplicatorAToB, backendA.BlobAccess, backendB, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		replicatorBToA, err := NewBlobReplicatorFromConfiguration(backend.Mirrored.ReplicatorBToA, backendB.BlobAccess, backendA, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      mirrored.NewMirroredBlobAccess(backendA.BlobAccess, backendB.BlobAccess, replicatorAToB, replicatorBToA),
			DigestKeyFormat: backendA.DigestKeyFormat.Combine(backendB.DigestKeyFormat),
		}, "mirrored", nil
	case *pb.BlobAccessConfiguration_Local:
		digestKeyFormat := creator.GetBaseDigestKeyFormat()
		var backendType string
		var sectorSizeBytes int
		var blockSectorCount int64
		var blockAllocator local.BlockAllocator
		switch dataBackend := backend.Local.DataBackend.(type) {
		case *pb.LocalBlobAccessConfiguration_InMemory_:
			backendType = "local_in_memory"
			// All data must be stored in memory. Because we
			// are not dealing with physical storage, there
			// is no need to take sector sizes into account.
			// Use a sector size of 1 byte to achieve
			// maximum storage density.
			sectorSizeBytes = 1
			blockSectorCount = dataBackend.InMemory.BlockSizeBytes
			blockAllocator = local.NewInMemoryBlockAllocator(int(dataBackend.InMemory.BlockSizeBytes))
		case *pb.LocalBlobAccessConfiguration_BlockDevice_:
			backendType = "local_block_device"
			// Data may be stored on a block device that is
			// memory mapped. Automatically determine the
			// block size based on the size of the block
			// device and the number of blocks.
			var f blockdevice.ReadWriterAt
			var sectorCount int64
			var err error
			f, sectorSizeBytes, sectorCount, err = blockdevice.MemoryMapBlockDevice(dataBackend.BlockDevice.Path)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrapf(err, "Failed to open block device %#v", dataBackend.BlockDevice.Path)
			}
			blockCount := dataBackend.BlockDevice.SpareBlocks + backend.Local.OldBlocks + backend.Local.CurrentBlocks + backend.Local.NewBlocks
			blockSectorCount = sectorCount / int64(blockCount)

			cachedReadBufferFactory := readBufferFactory
			if cacheConfiguration := dataBackend.BlockDevice.DataIntegrityValidationCache; cacheConfiguration != nil {
				dataIntegrityCheckingCache, err := digest.NewExistenceCacheFromConfiguration(cacheConfiguration, digestKeyFormat, "DataIntegrityValidationCache")
				if err != nil {
					return BlobAccessInfo{}, "", err
				}
				cachedReadBufferFactory = blobstore.NewValidationCachingReadBufferFactory(
					readBufferFactory,
					dataIntegrityCheckingCache)
			}

			blockAllocator = local.NewPartitioningBlockAllocator(
				f,
				cachedReadBufferFactory,
				sectorSizeBytes,
				blockSectorCount,
				int(blockCount))
		}

		implementation, err := local.NewLocalBlobAccess(
			local.NewHashingDigestLocationMap(
				local.NewInMemoryLocationRecordArray(int(backend.Local.DigestLocationMapSize)),
				int(backend.Local.DigestLocationMapSize),
				rand.Uint64(),
				backend.Local.DigestLocationMapMaximumGetAttempts,
				int(backend.Local.DigestLocationMapMaximumPutAttempts),
				storageTypeName),
			blockAllocator,
			util.DefaultErrorLogger,
			digestKeyFormat,
			storageTypeName,
			sectorSizeBytes,
			blockSectorCount,
			int(backend.Local.OldBlocks),
			int(backend.Local.CurrentBlocks),
			int(backend.Local.NewBlocks))
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      implementation,
			DigestKeyFormat: digestKeyFormat,
		}, backendType, nil
	case *pb.BlobAccessConfiguration_ReadFallback:
		primary, err := NewNestedBlobAccess(backend.ReadFallback.Primary, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		secondary, err := NewNestedBlobAccess(backend.ReadFallback.Secondary, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		replicator, err := NewBlobReplicatorFromConfiguration(backend.ReadFallback.Replicator, secondary.BlobAccess, primary, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      readfallback.NewReadFallbackBlobAccess(primary.BlobAccess, secondary.BlobAccess, replicator),
			DigestKeyFormat: primary.DigestKeyFormat.Combine(secondary.DigestKeyFormat),
		}, "read_fallback", nil
	case *pb.BlobAccessConfiguration_Demultiplexing:
		// Construct a trie for each of the backends specified
		// in the configuration indexed by instance name prefix.
		backendsTrie := digest.NewInstanceNameTrie()
		type demultiplexedBackendInfo struct {
			backend             blobstore.BlobAccess
			backendName         string
			instanceNamePatcher digest.InstanceNamePatcher
		}
		backends := make([]demultiplexedBackendInfo, 0, len(backend.Demultiplexing.InstanceNamePrefixes))
		for k, demultiplexed := range backend.Demultiplexing.InstanceNamePrefixes {
			matchInstanceNamePrefix, err := digest.NewInstanceName(k)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrapf(err, "Invalid instance name %#v", k)
			}
			addInstanceNamePrefix, err := digest.NewInstanceName(demultiplexed.AddInstanceNamePrefix)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrapf(err, "Invalid instance name %#v", demultiplexed.AddInstanceNamePrefix)
			}
			backend, err := NewNestedBlobAccess(demultiplexed.Backend, creator)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			backendsTrie.Set(matchInstanceNamePrefix, len(backends))
			backends = append(backends, demultiplexedBackendInfo{
				backend:             backend.BlobAccess,
				backendName:         matchInstanceNamePrefix.String(),
				instanceNamePatcher: digest.NewInstanceNamePatcher(matchInstanceNamePrefix, addInstanceNamePrefix),
			})
		}
		return BlobAccessInfo{
			BlobAccess: blobstore.NewDemultiplexingBlobAccess(
				func(i digest.InstanceName) (blobstore.BlobAccess, string, digest.InstanceNamePatcher, error) {
					idx := backendsTrie.Get(i)
					if idx < 0 {
						return nil, "", digest.NoopInstanceNamePatcher, status.Errorf(codes.InvalidArgument, "Unknown instance name: %#v", i.String())
					}
					return backends[idx].backend, backends[idx].backendName, backends[idx].instanceNamePatcher, nil
				}),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "demultiplexing", nil
	}
	return creator.NewCustomBlobAccess(configuration)
}

// NewNestedBlobAccess may be called by
// BlobAccessCreator.NewCustomBlobAccess() to create BlobAccess
// objects for instances nested inside the configuration.
func NewNestedBlobAccess(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (BlobAccessInfo, error) {
	if configuration == nil {
		return BlobAccessInfo{}, status.Error(codes.InvalidArgument, "Storage configuration not specified")
	}

	backend, backendType, err := newNestedBlobAccessBare(configuration, creator)
	if err != nil {
		return BlobAccessInfo{}, err
	}
	return BlobAccessInfo{
		BlobAccess:      blobstore.NewMetricsBlobAccess(backend.BlobAccess, clock.SystemClock, fmt.Sprintf("%s_%s", creator.GetStorageTypeName(), backendType)),
		DigestKeyFormat: backend.DigestKeyFormat,
	}, nil
}

// NewBlobAccessFromConfiguration creates a BlobAccess object based on a
// configuration file.
func NewBlobAccessFromConfiguration(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (BlobAccessInfo, error) {
	backend, err := NewNestedBlobAccess(configuration, creator)
	if err != nil {
		return BlobAccessInfo{}, err
	}
	return BlobAccessInfo{
		BlobAccess:      creator.WrapTopLevelBlobAccess(backend.BlobAccess),
		DigestKeyFormat: backend.DigestKeyFormat,
	}, nil
}

// NewCASAndACBlobAccessFromConfiguration is a convenience function to
// create BlobAccess objects for both the Content Addressable Storage
// and Action Cache. Most Buildbarn components tend to require access to
// both these data stores.
func NewCASAndACBlobAccessFromConfiguration(configuration *pb.BlobstoreConfiguration, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) (blobstore.BlobAccess, blobstore.BlobAccess, error) {
	contentAddressableStorage, err := NewBlobAccessFromConfiguration(
		configuration.GetContentAddressableStorage(),
		NewCASBlobAccessCreator(grpcClientFactory, maximumMessageSizeBytes))
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Content Addressable Storage")
	}

	actionCache, err := NewBlobAccessFromConfiguration(
		configuration.GetActionCache(),
		NewACBlobAccessCreator(
			contentAddressableStorage,
			grpcClientFactory,
			maximumMessageSizeBytes))
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Action Cache")
	}

	return contentAddressableStorage.BlobAccess, actionCache.BlobAccess, nil
}

func createCircularBlobAccess(config *pb.CircularBlobAccessConfiguration, creator BlobAccessCreator) (blobstore.BlobAccess, error) {
	// Open input files.
	circularDirectory, err := filesystem.NewLocalDirectory(config.Directory)
	if err != nil {
		return nil, err
	}
	defer circularDirectory.Close()
	dataFile, err := circularDirectory.OpenReadWrite("data", filesystem.CreateReuse(0644))
	if err != nil {
		return nil, err
	}
	stateFile, err := circularDirectory.OpenReadWrite("state", filesystem.CreateReuse(0644))
	if err != nil {
		return nil, err
	}

	var offsetStore circular.OffsetStore
	switch creator.GetBaseDigestKeyFormat() {
	case digest.KeyWithoutInstance:
		// Open a single offset file for all entries. This is
		// sufficient for the Content Addressable Storage.
		offsetFile, err := circularDirectory.OpenReadWrite("offset", filesystem.CreateReuse(0644))
		if err != nil {
			return nil, err
		}
		offsetStore = circular.NewCachingOffsetStore(
			circular.NewFileOffsetStore(offsetFile, config.OffsetFileSizeBytes),
			uint(config.OffsetCacheSize))
	case digest.KeyWithInstance:
		// Open an offset file for every instance. This is
		// required for the Action Cache.
		offsetStores := map[string]circular.OffsetStore{}
		for _, instance := range config.Instances {
			offsetFile, err := circularDirectory.OpenReadWrite("offset."+instance, filesystem.CreateReuse(0644))
			if err != nil {
				return nil, err
			}
			offsetStores[instance] = circular.NewCachingOffsetStore(
				circular.NewFileOffsetStore(offsetFile, config.OffsetFileSizeBytes),
				uint(config.OffsetCacheSize))
		}
		offsetStore = circular.NewDemultiplexingOffsetStore(func(instance string) (circular.OffsetStore, error) {
			offsetStore, ok := offsetStores[instance]
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "Unknown instance name")
			}
			return offsetStore, nil
		})
	}
	stateStore, err := circular.NewFileStateStore(stateFile, config.DataFileSizeBytes)
	if err != nil {
		return nil, err
	}

	return circular.NewCircularBlobAccess(
		offsetStore,
		circular.NewFileDataStore(dataFile, config.DataFileSizeBytes),
		circular.NewPositiveSizedBlobStateStore(
			circular.NewBulkAllocatingStateStore(
				stateStore,
				config.DataAllocationChunkSizeBytes)),
		creator.GetReadBufferFactory()), nil
}
