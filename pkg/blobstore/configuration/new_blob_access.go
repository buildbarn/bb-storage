package configuration

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/circular"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readcaching"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/clock"
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

// NewNestedBlobAccess may be called by
// BlobAccessCreator.NewCustomBlobAccess() to create BlobAccess
// objects for instances nested inside the configuration.
func NewNestedBlobAccess(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (blobstore.BlobAccess, error) {
	var implementation blobstore.BlobAccess
	var backendType string
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "Storage configuration not specified")
	}

	storageType := creator.GetStorageType()
	storageTypeName := creator.GetStorageTypeName()
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Circular:
		backendType = "circular"

		var err error
		implementation, err = createCircularBlobAccess(backend.Circular, creator)
		if err != nil {
			return nil, err
		}
	case *pb.BlobAccessConfiguration_Cloud:
		backendType = "cloud"
		switch backendConfig := backend.Cloud.Config.(type) {
		case *pb.CloudBlobAccessConfiguration_Url:
			ctx := context.Background()
			bucket, err := blob.OpenBucket(ctx, backendConfig.Url)
			if err != nil {
				return nil, err
			}
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, storageType)
		case *pb.CloudBlobAccessConfiguration_Azure:
			backendType = "azure"
			credential, err := azureblob.NewCredential(azureblob.AccountName(backendConfig.Azure.AccountName), azureblob.AccountKey(backendConfig.Azure.AccountKey))
			if err != nil {
				return nil, err
			}
			pipeline := azureblob.NewPipeline(credential, azblob.PipelineOptions{})
			ctx := context.Background()
			bucket, err := azureblob.OpenBucket(ctx, pipeline, azureblob.AccountName(backendConfig.Azure.AccountName), backendConfig.Azure.ContainerName, nil)
			if err != nil {
				return nil, err
			}
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, storageType)
		case *pb.CloudBlobAccessConfiguration_Gcs:
			backendType = "gcs"
			var creds *google.Credentials
			var err error
			ctx := context.Background()
			if backendConfig.Gcs.Credentials != "" {
				creds, err = google.CredentialsFromJSON(ctx, []byte(backendConfig.Gcs.Credentials), storage.ScopeReadWrite)
			} else {
				creds, err = google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
			}
			if err != nil {
				return nil, err
			}
			client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
			if err != nil {
				return nil, err
			}
			bucket, err := gcsblob.OpenBucket(ctx, client, backendConfig.Gcs.Bucket, nil)
			if err != nil {
				return nil, err
			}
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, storageType)
		case *pb.CloudBlobAccessConfiguration_S3:
			backendType = "s3"
			cfg := aws.Config{
				Endpoint:         &backendConfig.S3.Endpoint,
				Region:           &backendConfig.S3.Region,
				DisableSSL:       &backendConfig.S3.DisableSsl,
				S3ForcePathStyle: aws.Bool(true),
			}
			// If AccessKeyId isn't specified, allow AWS to search for credentials.
			// In AWS EC2, this search will include the instance IAM Role.
			if backendConfig.S3.AccessKeyId != "" {
				cfg.Credentials = credentials.NewStaticCredentials(backendConfig.S3.AccessKeyId, backendConfig.S3.SecretAccessKey, "")
			}
			session := session.New(&cfg)
			ctx := context.Background()
			bucket, err := s3blob.OpenBucket(ctx, session, backendConfig.S3.Bucket, nil)
			if err != nil {
				return nil, err
			}
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, storageType)
		default:
			return nil, status.Error(codes.InvalidArgument, "Cloud configuration did not contain a backend")
		}
	case *pb.BlobAccessConfiguration_Error:
		backendType = "error"
		implementation = blobstore.NewErrorBlobAccess(status.ErrorProto(backend.Error))
	case *pb.BlobAccessConfiguration_ReadCaching:
		backendType = "read_caching"
		slow, err := NewNestedBlobAccess(backend.ReadCaching.Slow, creator)
		if err != nil {
			return nil, err
		}
		fast, err := NewNestedBlobAccess(backend.ReadCaching.Fast, creator)
		if err != nil {
			return nil, err
		}
		replicator, err := NewBlobReplicatorFromConfiguration(backend.ReadCaching.Replicator, slow, fast, creator)
		if err != nil {
			return nil, err
		}
		implementation = readcaching.NewReadCachingBlobAccess(slow, fast, replicator)
	case *pb.BlobAccessConfiguration_Redis:
		backendType = "redis"

		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(backend.Redis.Tls)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to obtain TLS configuration")
		}

		var keyTTL time.Duration
		if backend.Redis.KeyTtl != nil {
			keyTTL, err = ptypes.Duration(backend.Redis.KeyTtl)
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to obtain key TTL configuration")
			}
		}

		var replicationTimeout time.Duration
		if backend.Redis.ReplicationTimeout != nil {
			replicationTimeout, err = ptypes.Duration(backend.Redis.ReplicationTimeout)
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to obtain replication timeout")
			}
		}

		var dialTimeout time.Duration
		if backend.Redis.DialTimeout != nil {
			dialTimeout, err = ptypes.Duration(backend.Redis.DialTimeout)
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to obtain dial timeout configuration")
			}
		}

		var readTimeout time.Duration
		if backend.Redis.ReadTimeout != nil {
			readTimeout, err = ptypes.Duration(backend.Redis.ReadTimeout)
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to obtain read timeout configuration")
			}
		}

		var writeTimeout time.Duration
		if backend.Redis.WriteTimeout != nil {
			writeTimeout, err = ptypes.Duration(backend.Redis.WriteTimeout)
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to obtain write timeout configuration")
			}
		}

		switch mode := backend.Redis.Mode.(type) {
		case *pb.RedisBlobAccessConfiguration_Clustered:
			// Gather retry configuration (min/max delay and overall retry attempts)
			minRetryDur := time.Millisecond * 32
			if mode.Clustered.MinimumRetryBackoff != nil {
				minRetryDur, err = ptypes.Duration(mode.Clustered.MinimumRetryBackoff)
				if err != nil {
					return nil, util.StatusWrap(err, "Failed to obtain minimum retry back off configuration")
				}
			}

			maxRetryDur := time.Millisecond * 2048
			if mode.Clustered.MaximumRetryBackoff != nil {
				maxRetryDur, err = ptypes.Duration(mode.Clustered.MaximumRetryBackoff)
				if err != nil {
					return nil, util.StatusWrap(err, "Failed to obtain maximum retry back off")
				}
			}

			maxRetries := 16 // Default will be 16
			if mode.Clustered.MaximumRetries != 0 {
				maxRetries = int(mode.Clustered.MaximumRetries)
			}

			implementation = blobstore.NewRedisBlobAccess(
				redis.NewClusterClient(
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
					}),
				creator.GetStorageType(),
				keyTTL,
				backend.Redis.ReplicationCount,
				replicationTimeout)
		case *pb.RedisBlobAccessConfiguration_Single:
			implementation = blobstore.NewRedisBlobAccess(
				redis.NewClient(
					&redis.Options{
						Addr:         mode.Single.Endpoint,
						Password:     mode.Single.Password,
						DB:           int(mode.Single.Db),
						TLSConfig:    tlsConfig,
						DialTimeout:  dialTimeout,
						ReadTimeout:  readTimeout,
						WriteTimeout: writeTimeout,
					}),
				creator.GetStorageType(),
				keyTTL,
				backend.Redis.ReplicationCount,
				replicationTimeout)
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Redis configuration must either be clustered or single server")
		}
	case *pb.BlobAccessConfiguration_Remote:
		backendType = "remote"
		implementation = blobstore.NewRemoteBlobAccess(backend.Remote.Address, storageTypeName, storageType)
	case *pb.BlobAccessConfiguration_Sharding:
		backendType = "sharding"
		backends := make([]blobstore.BlobAccess, 0, len(backend.Sharding.Shards))
		weights := make([]uint32, 0, len(backend.Sharding.Shards))
		hasUndrainedBackend := false
		for _, shard := range backend.Sharding.Shards {
			if shard.Backend == nil {
				// Drained backend.
				backends = append(backends, nil)
			} else {
				// Undrained backend.
				backend, err := NewNestedBlobAccess(shard.Backend, creator)
				if err != nil {
					return nil, err
				}
				backends = append(backends, backend)
				hasUndrainedBackend = true
			}

			if shard.Weight == 0 {
				return nil, status.Errorf(codes.InvalidArgument, "Shards must have positive weights")
			}
			weights = append(weights, shard.Weight)
		}
		if !hasUndrainedBackend {
			return nil, status.Errorf(codes.InvalidArgument, "Cannot create sharding blob access without any undrained backends")
		}
		implementation = sharding.NewShardingBlobAccess(
			backends,
			sharding.NewWeightedShardPermuter(weights),
			creator.GetStorageType(),
			backend.Sharding.HashInitialization)
	case *pb.BlobAccessConfiguration_SizeDistinguishing:
		backendType = "size_distinguishing"
		small, err := NewNestedBlobAccess(backend.SizeDistinguishing.Small, creator)
		if err != nil {
			return nil, err
		}
		large, err := NewNestedBlobAccess(backend.SizeDistinguishing.Large, creator)
		if err != nil {
			return nil, err
		}
		implementation = blobstore.NewSizeDistinguishingBlobAccess(small, large, backend.SizeDistinguishing.CutoffSizeBytes)
	case *pb.BlobAccessConfiguration_Mirrored:
		backendType = "mirrored"
		backendA, err := NewNestedBlobAccess(backend.Mirrored.BackendA, creator)
		if err != nil {
			return nil, err
		}
		backendB, err := NewNestedBlobAccess(backend.Mirrored.BackendB, creator)
		if err != nil {
			return nil, err
		}
		replicatorAToB, err := NewBlobReplicatorFromConfiguration(backend.Mirrored.ReplicatorAToB, backendA, backendB, creator)
		if err != nil {
			return nil, err
		}
		replicatorBToA, err := NewBlobReplicatorFromConfiguration(backend.Mirrored.ReplicatorBToA, backendB, backendA, creator)
		if err != nil {
			return nil, err
		}
		implementation = mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)
	case *pb.BlobAccessConfiguration_Local:
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
				return nil, util.StatusWrapf(err, "Failed to open block device %#v", dataBackend.BlockDevice.Path)
			}
			blockCount := dataBackend.BlockDevice.SpareBlocks + backend.Local.OldBlocks + backend.Local.CurrentBlocks + backend.Local.NewBlocks
			blockSectorCount = sectorCount / int64(blockCount)
			blockAllocator = local.NewPartitioningBlockAllocator(
				f,
				creator.GetStorageType(),
				sectorSizeBytes,
				blockSectorCount,
				int(blockCount),
				dataBackend.BlockDevice.DisableIntegrityChecking)
		}

		var err error
		implementation, err = local.NewLocalBlobAccess(
			local.NewHashingDigestLocationMap(
				local.NewInMemoryLocationRecordArray(int(backend.Local.DigestLocationMapSize)),
				int(backend.Local.DigestLocationMapSize),
				rand.Uint64(),
				backend.Local.DigestLocationMapMaximumGetAttempts,
				int(backend.Local.DigestLocationMapMaximumPutAttempts),
				storageTypeName),
			blockAllocator,
			storageType,
			storageTypeName,
			sectorSizeBytes,
			blockSectorCount,
			int(backend.Local.OldBlocks),
			int(backend.Local.CurrentBlocks),
			int(backend.Local.NewBlocks))
		if err != nil {
			return nil, err
		}
	case *pb.BlobAccessConfiguration_ReadFallback:
		backendType = "read_fallback"
		primary, err := NewNestedBlobAccess(backend.ReadFallback.Primary, creator)
		if err != nil {
			return nil, err
		}
		secondary, err := NewNestedBlobAccess(backend.ReadFallback.Secondary, creator)
		if err != nil {
			return nil, err
		}
		implementation = blobstore.NewReadFallbackBlobAccess(primary, secondary)
	default:
		var err error
		implementation, backendType, err = creator.NewCustomBlobAccess(configuration)
		if err != nil {
			return nil, err
		}
	}
	return blobstore.NewMetricsBlobAccess(implementation, clock.SystemClock, fmt.Sprintf("%s_%s", storageTypeName, backendType)), nil
}

// NewBlobAccessFromConfiguration creates a BlobAccess object based on a
// configuration file.
func NewBlobAccessFromConfiguration(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (blobstore.BlobAccess, error) {
	blobAccess, err := NewNestedBlobAccess(configuration, creator)
	if err != nil {
		return nil, err
	}
	return creator.WrapTopLevelBlobAccess(blobAccess), nil
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

	return contentAddressableStorage, actionCache, nil
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
	switch creator.GetDigestKeyFormat() {
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
		creator.GetStorageType()), nil
}
