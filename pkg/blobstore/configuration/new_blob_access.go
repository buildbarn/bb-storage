package configuration

import (
	"archive/zip"
	"context"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readcaching"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readfallback"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	digest_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/digest"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/fxtlabs/primes"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BlobAccessInfo contains an instance of BlobAccess and information
// relevant to its creation. It is returned by functions that construct
// BlobAccess instances, such as NewBlobAccessFromConfiguration().
type BlobAccessInfo struct {
	BlobAccess      blobstore.BlobAccess
	DigestKeyFormat digest.KeyFormat
}

func newCachedReadBufferFactory(cacheConfiguration *digest_pb.ExistenceCacheConfiguration, baseReadBufferFactory blobstore.ReadBufferFactory, digestKeyFormat digest.KeyFormat) (blobstore.ReadBufferFactory, error) {
	if cacheConfiguration == nil {
		// No caching enabled.
		return baseReadBufferFactory, nil
	}
	dataIntegrityCheckingCache, err := digest.NewExistenceCacheFromConfiguration(cacheConfiguration, digestKeyFormat, "DataIntegrityValidationCache")
	if err != nil {
		return nil, err
	}
	return blobstore.NewValidationCachingReadBufferFactory(
		baseReadBufferFactory,
		dataIntegrityCheckingCache), nil
}

type simpleNestedBlobAccessCreator struct {
	terminationGroup program.Group
	labels           map[string]BlobAccessInfo
}

func (nc *simpleNestedBlobAccessCreator) newNestedBlobAccessBare(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (BlobAccessInfo, string, error) {
	readBufferFactory := creator.GetReadBufferFactory()
	storageTypeName := creator.GetStorageTypeName()
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Error:
		return BlobAccessInfo{
			BlobAccess:      blobstore.NewErrorBlobAccess(status.ErrorProto(backend.Error)),
			DigestKeyFormat: digest.KeyWithoutInstance,
		}, "error", nil
	case *pb.BlobAccessConfiguration_ReadCaching:
		slow, err := nc.NewNestedBlobAccess(backend.ReadCaching.Slow, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		fast, err := nc.NewNestedBlobAccess(backend.ReadCaching.Fast, creator)
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
	case *pb.BlobAccessConfiguration_Http:
		roundTripper, err := bb_http.NewRoundTripperFromConfiguration(backend.Http.Client)
		if err != nil {
			return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create HTTP client")
		}
		return BlobAccessInfo{
			BlobAccess: blobstore.NewHTTPBlobAccess(
				backend.Http.Address,
				storageTypeName,
				readBufferFactory,
				&http.Client{
					Transport: bb_http.NewMetricsRoundTripper(roundTripper, "HTTPBlobAccess"),
				},
				creator.GetDefaultCapabilitiesProvider()),
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
				backend, err := nc.NewNestedBlobAccess(shard.Backend, creator)
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
				backend.Sharding.HashInitialization),
			DigestKeyFormat: *combinedDigestKeyFormat,
		}, "sharding", nil
	case *pb.BlobAccessConfiguration_Mirrored:
		backendA, err := nc.NewNestedBlobAccess(backend.Mirrored.BackendA, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		backendB, err := nc.NewNestedBlobAccess(backend.Mirrored.BackendB, creator)
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
		digestKeyFormat := digest.KeyWithInstance
		if !backend.Local.HierarchicalInstanceNames {
			digestKeyFormat = creator.GetBaseDigestKeyFormat()
		}
		persistent := backend.Local.Persistent

		// Create the backing store for blocks of data.
		var backendType string
		var sectorSizeBytes int
		var blockSectorCount int64
		var blockAllocator local.BlockAllocator
		dataSyncer := func() error { return nil }
		switch blocksBackend := backend.Local.BlocksBackend.(type) {
		case *pb.LocalBlobAccessConfiguration_BlocksInMemory_:
			backendType = "local_in_memory"
			// All data must be stored in memory. Because we
			// are not dealing with physical storage, there
			// is no need to take sector sizes into account.
			// Use a sector size of 1 byte to achieve
			// maximum storage density.
			sectorSizeBytes = 1
			blockSectorCount = blocksBackend.BlocksInMemory.BlockSizeBytes
			blockAllocator = local.NewInMemoryBlockAllocator(int(blocksBackend.BlocksInMemory.BlockSizeBytes))
		case *pb.LocalBlobAccessConfiguration_BlocksOnBlockDevice_:
			backendType = "local_block_device"
			// Data may be stored on a block device that is
			// memory mapped. Automatically determine the
			// block size based on the size of the block
			// device and the number of blocks.
			blocksOnBlockDevice := blocksBackend.BlocksOnBlockDevice
			var blockDevice blockdevice.BlockDevice
			var sectorCount int64
			var err error
			blockDevice, sectorSizeBytes, sectorCount, err = blockdevice.NewBlockDeviceFromConfiguration(
				blocksOnBlockDevice.Source,
				persistent == nil)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to open blocks block device")
			}
			dataSyncer = blockDevice.Sync
			blockCount := blocksOnBlockDevice.SpareBlocks + backend.Local.OldBlocks + backend.Local.CurrentBlocks + backend.Local.NewBlocks
			blockSectorCount = sectorCount / int64(blockCount)
			if blockSectorCount <= 0 {
				return BlobAccessInfo{}, "", status.Errorf(codes.InvalidArgument, "Block device only has %d sectors (%d bytes each), which is less than the total number of blocks (%d), meaning this backend would be incapable of storing any data", sectorCount, sectorSizeBytes, blockCount)
			}

			cachedReadBufferFactory, err := newCachedReadBufferFactory(blocksOnBlockDevice.DataIntegrityValidationCache, readBufferFactory, digestKeyFormat)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}

			blockAllocator = local.NewBlockDeviceBackedBlockAllocator(
				blockDevice,
				cachedReadBufferFactory,
				sectorSizeBytes,
				blockSectorCount,
				int(blockCount),
				storageTypeName)
		default:
			return BlobAccessInfo{}, "", status.Error(codes.InvalidArgument, "Blocks backend not specified")
		}

		var globalLock sync.RWMutex
		var blockList local.BlockList
		var keyLocationMapHashInitialization uint64
		initialBlockCount := 0
		if persistent == nil {
			// Persistency is disabled. Provide a simple
			// volatile BlockList.
			blockList = local.NewVolatileBlockList(blockAllocator)
			keyLocationMapHashInitialization = random.CryptoThreadSafeGenerator.Uint64()
		} else {
			// Persistency is enabled. Reload previous
			// persistent state from disk.
			persistentStateDirectory, err := filesystem.NewLocalDirectory(persistent.StateDirectoryPath)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrapf(err, "Failed to open persistent state directory %#v", persistent.StateDirectoryPath)
			}
			persistentStateStore := local.NewDirectoryBackedPersistentStateStore(persistentStateDirectory)
			persistentState, err := persistentStateStore.ReadPersistentState()
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrapf(err, "Failed to reload persistent state from %#v", persistent.StateDirectoryPath)
			}
			keyLocationMapHashInitialization = persistentState.KeyLocationMapHashInitialization

			// Create a persistent BlockList. This will
			// attempt to reattach the old blocks. The
			// number of valid blocks is returned, so that
			// the dimensions of the OldNewCurrentLocationBlobMap
			// can be set properly.
			var persistentBlockList *local.PersistentBlockList
			persistentBlockList, initialBlockCount = local.NewPersistentBlockList(
				blockAllocator,
				persistentState.OldestEpochId,
				persistentState.Blocks)
			blockList = persistentBlockList

			// Start goroutines that update the persistent
			// state file when writes and block releases
			// occur.
			if err := persistent.MinimumEpochInterval.CheckValid(); err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to obtain minimum epoch duration")
			}
			minimumEpochInterval := persistent.MinimumEpochInterval.AsDuration()
			periodicSyncer := local.NewPeriodicSyncer(
				persistentBlockList,
				&globalLock,
				persistentStateStore,
				clock.SystemClock,
				util.DefaultErrorLogger,
				10*time.Second,
				minimumEpochInterval,
				keyLocationMapHashInitialization,
				dataSyncer)
			// TODO: Run this as part of the program.Group,
			// so that it gets cleaned up upon shutdown.
			go func() {
				for {
					periodicSyncer.ProcessBlockRelease()
				}
			}()
			nc.terminationGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				for periodicSyncer.ProcessBlockPut(ctx) {
				}
				// TODO: Let PeriodicSyncer propagate errors
				// upwards in case they occur after the context
				// has been cancelled.
				return nil
			})
		}

		blockListGrowthPolicy, err := creator.NewBlockListGrowthPolicy(
			int(backend.Local.CurrentBlocks),
			int(backend.Local.NewBlocks))
		if err != nil {
			return BlobAccessInfo{}, "", err
		}

		locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
			blockList,
			blockListGrowthPolicy,
			util.DefaultErrorLogger,
			storageTypeName,
			int64(sectorSizeBytes)*blockSectorCount,
			int(backend.Local.OldBlocks),
			int(backend.Local.NewBlocks),
			initialBlockCount)

		// Create the backing store for the key-location map.
		var locationRecordArraySize int
		var locationRecordArray local.LocationRecordArray
		switch keyLocationMapBackend := backend.Local.KeyLocationMapBackend.(type) {
		case *pb.LocalBlobAccessConfiguration_KeyLocationMapInMemory_:
			locationRecordArraySize = int(keyLocationMapBackend.KeyLocationMapInMemory.Entries)
			locationRecordArray = local.NewInMemoryLocationRecordArray(
				locationRecordArraySize,
				locationBlobMap)
		case *pb.LocalBlobAccessConfiguration_KeyLocationMapOnBlockDevice:
			blockDevice, sectorSizeBytes, sectorCount, err := blockdevice.NewBlockDeviceFromConfiguration(
				keyLocationMapBackend.KeyLocationMapOnBlockDevice,
				persistent == nil)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to open key-location map block device")
			}
			locationRecordArraySize = int((int64(sectorSizeBytes) * sectorCount) / local.BlockDeviceBackedLocationRecordSize)
			locationRecordArray = local.NewBlockDeviceBackedLocationRecordArray(
				blockDevice,
				locationBlobMap)
		default:
			return BlobAccessInfo{}, "", status.Errorf(codes.InvalidArgument, "Key-location map backend not specified")
		}

		// Considering that FNV-1a is used to compute keys and
		// HashingKeyLocationMap uses simple modulo arithmetic
		// to store entries in the location record array, ensure
		// that the size that is used is prime. This causes the
		// best dispersion of hash table entries.
		for locationRecordArraySize > 3 && !primes.IsPrime(locationRecordArraySize) {
			locationRecordArraySize--
		}

		keyLocationMap := local.NewHashingKeyLocationMap(
			locationRecordArray,
			locationRecordArraySize,
			keyLocationMapHashInitialization,
			backend.Local.KeyLocationMapMaximumGetAttempts,
			int(backend.Local.KeyLocationMapMaximumPutAttempts),
			storageTypeName)

		var localBlobAccess blobstore.BlobAccess
		if backend.Local.HierarchicalInstanceNames {
			localBlobAccess, err = creator.NewHierarchicalInstanceNamesLocalBlobAccess(
				keyLocationMap,
				locationBlobMap,
				&globalLock)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
		} else {
			localBlobAccess = local.NewFlatBlobAccess(
				keyLocationMap,
				locationBlobMap,
				digestKeyFormat,
				&globalLock,
				storageTypeName,
				creator.GetDefaultCapabilitiesProvider())
		}
		return BlobAccessInfo{
			BlobAccess:      localBlobAccess,
			DigestKeyFormat: digestKeyFormat,
		}, backendType, nil
	case *pb.BlobAccessConfiguration_ReadFallback:
		primary, err := nc.NewNestedBlobAccess(backend.ReadFallback.Primary, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		secondary, err := nc.NewNestedBlobAccess(backend.ReadFallback.Secondary, creator)
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
			backend, err := nc.NewNestedBlobAccess(demultiplexed.Backend, creator)
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
					idx := backendsTrie.GetLongestPrefix(i)
					if idx < 0 {
						return nil, "", digest.NoopInstanceNamePatcher, status.Errorf(codes.InvalidArgument, "Unknown instance name: %#v", i.String())
					}
					return backends[idx].backend, backends[idx].backendName, backends[idx].instanceNamePatcher, nil
				}),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "demultiplexing", nil
	case *pb.BlobAccessConfiguration_ReadCanarying:
		config := backend.ReadCanarying
		source, err := nc.NewNestedBlobAccess(config.Source, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		replica, err := nc.NewNestedBlobAccess(config.Replica, creator)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		maximumCacheDuration := config.MaximumCacheDuration
		if err := maximumCacheDuration.CheckValid(); err != nil {
			return BlobAccessInfo{}, "", util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid maximum cache duration")
		}
		return BlobAccessInfo{
			BlobAccess: blobstore.NewReadCanaryingBlobAccess(
				source.BlobAccess,
				replica.BlobAccess,
				clock.SystemClock,
				eviction.NewMetricsSet(eviction.NewLRUSet[string](), "ReadCanaryingBlobAccess"),
				int(config.MaximumCacheSize),
				maximumCacheDuration.AsDuration(),
				util.DefaultErrorLogger),
			DigestKeyFormat: source.DigestKeyFormat.Combine(replica.DigestKeyFormat),
		}, "read_canarying", nil
	case *pb.BlobAccessConfiguration_ZipReading:
		config := backend.ZipReading
		file, err := os.Open(config.Path)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		fileInfo, err := file.Stat()
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		zipReader, err := zip.NewReader(file, fileInfo.Size())
		if err != nil {
			file.Close()
			return BlobAccessInfo{}, "", util.StatusWrapf(err, "Failed to open ZIP file %#v", config.Path)
		}

		digestKeyFormat := creator.GetBaseDigestKeyFormat()
		cachedReadBufferFactory, err := newCachedReadBufferFactory(config.DataIntegrityValidationCache, readBufferFactory, digestKeyFormat)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}

		return BlobAccessInfo{
			BlobAccess: blobstore.NewZIPReadingBlobAccess(
				creator.GetDefaultCapabilitiesProvider(),
				cachedReadBufferFactory,
				digestKeyFormat,
				zipReader.File),
			DigestKeyFormat: digestKeyFormat,
		}, "zip_reading", nil
	case *pb.BlobAccessConfiguration_ZipWriting:
		config := backend.ZipWriting
		zipPath := config.Path
		file, err := os.OpenFile(zipPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		digestKeyFormat := creator.GetBaseDigestKeyFormat()
		cachedReadBufferFactory, err := newCachedReadBufferFactory(config.DataIntegrityValidationCache, readBufferFactory, digestKeyFormat)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		blobAccess := blobstore.NewZIPWritingBlobAccess(
			creator.GetDefaultCapabilitiesProvider(),
			cachedReadBufferFactory,
			digestKeyFormat,
			file)

		// Ensure the central directory is written upon termination.
		nc.terminationGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			<-ctx.Done()
			if err := blobAccess.Finalize(); err != nil {
				return util.StatusWrapf(err, "Failed to finalize ZIP archive %#v", zipPath)
			}
			if err := file.Sync(); err != nil {
				return util.StatusWrapf(err, "Failed to synchronize ZIP archive %#v", zipPath)
			}
			return nil
		})

		return BlobAccessInfo{
			BlobAccess:      blobAccess,
			DigestKeyFormat: digestKeyFormat,
		}, "zip_writing", nil
	}
	return creator.NewCustomBlobAccess(configuration, nc)
}

// NewNestedBlobAccess may be called by
// BlobAccessCreator.NewCustomBlobAccess() to create BlobAccess
// objects for instances nested inside the configuration.
func (nc *simpleNestedBlobAccessCreator) NewNestedBlobAccess(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (BlobAccessInfo, error) {
	if configuration == nil {
		return BlobAccessInfo{}, status.Error(codes.InvalidArgument, "Storage configuration not specified")
	}

	// Protobuf does not support anchors/aliases like YAML. Have
	// separate 'with_labels' and 'labels' backends that can be used
	// to declare anchors and aliases, respectively.
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_WithLabels:
		config := backend.WithLabels

		// Inherit labels from the parent.
		labels := map[string]BlobAccessInfo{}
		for label, labelBackend := range nc.labels {
			labels[label] = labelBackend
		}

		// Add additional labels declared in config.
		for label, labelBackend := range config.Labels {
			if _, ok := labels[label]; ok {
				// Disallow shadowing.
				return BlobAccessInfo{}, status.Errorf(codes.InvalidArgument, "Label %#v has already been declared", label)
			}
			info, err := nc.NewNestedBlobAccess(labelBackend, creator)
			if err != nil {
				return BlobAccessInfo{}, util.StatusWrapf(err, "Label %#v", label)
			}
			labels[label] = info
		}

		return (&simpleNestedBlobAccessCreator{
			terminationGroup: nc.terminationGroup,
			labels:           labels,
		}).NewNestedBlobAccess(config.Backend, creator)
	case *pb.BlobAccessConfiguration_Label:
		if labelBackend, ok := nc.labels[backend.Label]; ok {
			return labelBackend, nil
		}
		return BlobAccessInfo{}, status.Errorf(codes.InvalidArgument, "Label %#v not declared", backend.Label)
	}

	backend, backendType, err := nc.newNestedBlobAccessBare(configuration, creator)
	if err != nil {
		return BlobAccessInfo{}, err
	}
	return BlobAccessInfo{
		BlobAccess:      blobstore.NewMetricsBlobAccess(backend.BlobAccess, clock.SystemClock, creator.GetStorageTypeName(), backendType),
		DigestKeyFormat: backend.DigestKeyFormat,
	}, nil
}

// NewBlobAccessFromConfiguration creates a BlobAccess object based on a
// configuration file.
func NewBlobAccessFromConfiguration(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator) (BlobAccessInfo, error) {
	nestedCreator := &simpleNestedBlobAccessCreator{
		terminationGroup: terminationGroup,
	}
	backend, err := nestedCreator.NewNestedBlobAccess(configuration, creator)
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
func NewCASAndACBlobAccessFromConfiguration(terminationGroup program.Group, configuration *pb.BlobstoreConfiguration, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) (blobstore.BlobAccess, blobstore.BlobAccess, error) {
	contentAddressableStorage, err := NewBlobAccessFromConfiguration(
		terminationGroup,
		configuration.GetContentAddressableStorage(),
		NewCASBlobAccessCreator(grpcClientFactory, maximumMessageSizeBytes))
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Content Addressable Storage")
	}

	actionCache, err := NewBlobAccessFromConfiguration(
		terminationGroup,
		configuration.GetActionCache(),
		NewACBlobAccessCreator(
			&contentAddressableStorage,
			grpcClientFactory,
			maximumMessageSizeBytes))
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Action Cache")
	}

	return contentAddressableStorage.BlobAccess, actionCache.BlobAccess, nil
}
