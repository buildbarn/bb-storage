package configuration

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/circular"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"

	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/storage"
)

// CreateBlobAccessObjectsFromConfig creates a pair of BlobAccess
// objects for the Content Addressable Storage and Action cache based on
// a configuration file.
func CreateBlobAccessObjectsFromConfig(configurationFile string) (blobstore.BlobAccess, blobstore.BlobAccess, error) {
	data, err := ioutil.ReadFile(configurationFile)
	if err != nil {
		return nil, nil, err
	}
	var config pb.BlobstoreConfiguration
	if err := proto.UnmarshalText(string(data), &config); err != nil {
		return nil, nil, err
	}

	// Create two stores based on definitions in configuration.
	contentAddressableStorage, err := createBlobAccess(config.ContentAddressableStorage, "cas", util.DigestKeyWithoutInstance)
	if err != nil {
		return nil, nil, err
	}
	actionCache, err := createBlobAccess(config.ActionCache, "ac", util.DigestKeyWithInstance)
	if err != nil {
		return nil, nil, err
	}

	// Stack a mandatory layer on top to protect against data corruption.
	contentAddressableStorage = blobstore.NewMetricsBlobAccess(
		blobstore.NewMerkleBlobAccess(contentAddressableStorage),
		"cas_merkle")
	return contentAddressableStorage, actionCache, nil
}

func createBlobAccess(config *pb.BlobAccessConfiguration, storageType string, digestKeyFormat util.DigestKeyFormat) (blobstore.BlobAccess, error) {
	var implementation blobstore.BlobAccess
	var backendType string
	if config == nil {
		return nil, errors.New("Configuration not specified")
	}
	switch backend := config.Backend.(type) {
	case *pb.BlobAccessConfiguration_Circular:
		backendType = "circular"

		// Open input files.
		circularDirectory, err := filesystem.NewLocalDirectory(backend.Circular.Directory)
		if err != nil {
			return nil, err
		}
		defer circularDirectory.Close()
		dataFile, err := circularDirectory.OpenFile("data", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		stateFile, err := circularDirectory.OpenFile("state", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}

		var offsetStore circular.OffsetStore
		switch digestKeyFormat {
		case util.DigestKeyWithoutInstance:
			// Open a single offset file for all entries. This is
			// sufficient for the Content Addressable Storage.
			offsetFile, err := circularDirectory.OpenFile("offset", os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			offsetStore = circular.NewCachingOffsetStore(
				circular.NewFileOffsetStore(offsetFile, backend.Circular.OffsetFileSizeBytes),
				uint(backend.Circular.OffsetCacheSize))
		case util.DigestKeyWithInstance:
			// Open an offset file for every instance. This is
			// required for the Action Cache.
			offsetStores := map[string]circular.OffsetStore{}
			for _, instance := range backend.Circular.Instance {
				offsetFile, err := circularDirectory.OpenFile("offset."+instance, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					return nil, err
				}
				offsetStores[instance] = circular.NewCachingOffsetStore(
					circular.NewFileOffsetStore(offsetFile, backend.Circular.OffsetFileSizeBytes),
					uint(backend.Circular.OffsetCacheSize))
			}
			offsetStore = circular.NewDemultiplexingOffsetStore(func(instance string) (circular.OffsetStore, error) {
				offsetStore, ok := offsetStores[instance]
				if !ok {
					return nil, status.Errorf(codes.InvalidArgument, "Unknown instance name")
				}
				return offsetStore, nil
			})
		}
		stateStore, err := circular.NewFileStateStore(stateFile, backend.Circular.DataFileSizeBytes)
		if err != nil {
			return nil, err
		}

		implementation = circular.NewCircularBlobAccess(
			offsetStore,
			circular.NewFileDataStore(dataFile, backend.Circular.DataFileSizeBytes),
			circular.NewPositiveSizedBlobStateStore(
				circular.NewBulkAllocatingStateStore(
					stateStore,
					backend.Circular.DataAllocationChunkSizeBytes)))
	case *pb.BlobAccessConfiguration_Cloud:
		backendType = "cloud"
		switch backendConfig := backend.Cloud.Config.(type) {
		case *pb.CloudBlobAccessConfiguration_Url:
			ctx := context.Background()
			bucket, err := blob.OpenBucket(ctx, backendConfig.Url)
			if err != nil {
				return nil, err
			}
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, digestKeyFormat)
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
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, digestKeyFormat)
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
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, digestKeyFormat)
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
			implementation = blobstore.NewCloudBlobAccess(bucket, backend.Cloud.KeyPrefix, digestKeyFormat)
		default:
			return nil, errors.New("Cloud configuration did not contain a backend")
		}
	case *pb.BlobAccessConfiguration_Error:
		backendType = "failing"
		implementation = blobstore.NewErrorBlobAccess(status.ErrorProto(backend.Error))
	case *pb.BlobAccessConfiguration_Grpc:
		backendType = "grpc"
		client, err := grpc.Dial(
			backend.Grpc.Endpoint,
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
			grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
		if err != nil {
			return nil, err
		}
		switch storageType {
		case "ac":
			implementation = blobstore.NewActionCacheBlobAccess(client)
		case "cas":
			implementation = blobstore.NewContentAddressableStorageBlobAccess(client, 65536)
		}
	case *pb.BlobAccessConfiguration_Redis:
		backendType = "redis"
		implementation = blobstore.NewRedisBlobAccess(
			redis.NewClient(
				&redis.Options{
					Addr: backend.Redis.Endpoint,
					DB:   int(backend.Redis.Db),
				}),
			digestKeyFormat)
	case *pb.BlobAccessConfiguration_Remote:
		backendType = "remote"
		implementation = blobstore.NewRemoteBlobAccess(backend.Remote.Address, storageType)
	case *pb.BlobAccessConfiguration_Sharding:
		backendType = "sharding"
		var backends []blobstore.BlobAccess
		var weights []uint32
		hasUndrainedBackend := false
		for _, shard := range backend.Sharding.Shard {
			if shard.Backend == nil {
				// Drained backend.
				backends = append(backends, nil)
			} else {
				// Undrained backend.
				backend, err := createBlobAccess(shard.Backend, storageType, digestKeyFormat)
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
			digestKeyFormat,
			backend.Sharding.HashInitialization)
	case *pb.BlobAccessConfiguration_SizeDistinguishing:
		backendType = "size_distinguishing"
		small, err := createBlobAccess(backend.SizeDistinguishing.Small, storageType, digestKeyFormat)
		if err != nil {
			return nil, err
		}
		large, err := createBlobAccess(backend.SizeDistinguishing.Large, storageType, digestKeyFormat)
		if err != nil {
			return nil, err
		}
		implementation = blobstore.NewSizeDistinguishingBlobAccess(small, large, backend.SizeDistinguishing.CutoffSizeBytes)
	default:
		return nil, errors.New("Configuration did not contain a backend")
	}
	return blobstore.NewMetricsBlobAccess(implementation, fmt.Sprintf("%s_%s", storageType, backendType)), nil
}
