package configuration

import (
	"archive/zip"
	"context"
	"math"
	"os"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readcaching"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readfallback"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/lossymap"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BlobAccessInfo contains an instance of BlobAccess and information
// relevant to its creation. It is returned by functions that construct
// BlobAccess instances, such as NewBlobAccessFromConfiguration().
type BlobAccessInfo[T any] struct {
	BlobAccess      blobstore.BlobAccess[T]
	DigestKeyFormat digest.KeyFormat
}

type simpleNestedBlobAccessCreator[T any] struct {
	terminationGroup program.Group
	labels           map[string]BlobAccessInfo[T]
}

func (nc *simpleNestedBlobAccessCreator[T]) newNestedBlobAccessBare(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator[T]) (BlobAccessInfo[T], string, error) {
	storageTypeName := creator.GetStorageTypeName()
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Error:
		return BlobAccessInfo[T]{
			BlobAccess:      blobstore.NewErrorBlobAccess[T](status.ErrorProto(backend.Error)),
			DigestKeyFormat: digest.KeyWithoutInstance,
		}, "error", nil
	case *pb.BlobAccessConfiguration_ReadCaching:
		slow, err := nc.NewNestedBlobAccess(backend.ReadCaching.Slow, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		fast, err := nc.NewNestedBlobAccess(backend.ReadCaching.Fast, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		replicator, err := NewBlobReplicatorFromConfiguration(nc.terminationGroup, backend.ReadCaching.Replicator, slow.BlobAccess, fast, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		return BlobAccessInfo[T]{
			BlobAccess:      readcaching.NewReadCachingBlobAccess(slow.BlobAccess, fast.BlobAccess, replicator),
			DigestKeyFormat: slow.DigestKeyFormat,
		}, "read_caching", nil
	case *pb.BlobAccessConfiguration_Sharding:
		backends := make([]sharding.ShardBackend[T], 0, len(backend.Sharding.Shards))
		shards := make([]sharding.Shard, 0, len(backend.Sharding.Shards))
		keys := make([]string, 0, len(backend.Sharding.Shards))
		var combinedDigestKeyFormat *digest.KeyFormat
		for key, shard := range backend.Sharding.Shards {
			backend, err := nc.NewNestedBlobAccess(shard.Backend, creator)
			if err != nil {
				return BlobAccessInfo[T]{}, "", err
			}
			backends = append(backends, sharding.ShardBackend[T]{Backend: backend.BlobAccess, Key: key})
			if combinedDigestKeyFormat == nil {
				combinedDigestKeyFormat = &backend.DigestKeyFormat
			} else {
				newDigestKeyFormat := combinedDigestKeyFormat.Combine(backend.DigestKeyFormat)
				combinedDigestKeyFormat = &newDigestKeyFormat
			}

			if shard.Weight == 0 {
				return BlobAccessInfo[T]{}, "", status.Errorf(codes.InvalidArgument, "Shards must have positive weights")
			}
			shards = append(shards, sharding.Shard{
				Key:    key,
				Weight: shard.Weight,
			})
			keys = append(keys, key)
		}
		if combinedDigestKeyFormat == nil {
			return BlobAccessInfo[T]{}, "", status.Errorf(codes.InvalidArgument, "Cannot create sharding blob access without any backends")
		}
		shardSelector, err := sharding.NewRendezvousShardSelector(shards)
		if err != nil {
			return BlobAccessInfo[T]{}, "", status.Errorf(codes.InvalidArgument, "Could not create rendezvous shard selector")
		}
		return BlobAccessInfo[T]{
			BlobAccess: sharding.NewShardingBlobAccess(
				backends,
				shardSelector,
			),
			DigestKeyFormat: *combinedDigestKeyFormat,
		}, "sharding", nil
	case *pb.BlobAccessConfiguration_Mirrored:
		backendA, err := nc.NewNestedBlobAccess(backend.Mirrored.BackendA, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		backendB, err := nc.NewNestedBlobAccess(backend.Mirrored.BackendB, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		replicatorAToB, err := NewBlobReplicatorFromConfiguration(nc.terminationGroup, backend.Mirrored.ReplicatorAToB, backendA.BlobAccess, backendB, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		replicatorBToA, err := NewBlobReplicatorFromConfiguration(nc.terminationGroup, backend.Mirrored.ReplicatorBToA, backendB.BlobAccess, backendA, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		return BlobAccessInfo[T]{
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
				persistent == nil,
			)
			if err != nil {
				return BlobAccessInfo[T]{}, "", util.StatusWrap(err, "Failed to open blocks block device")
			}
			dataSyncer = blockDevice.Sync
			blockCount := blocksOnBlockDevice.SpareBlocks + backend.Local.OldBlocks + backend.Local.CurrentBlocks + backend.Local.NewBlocks
			if blockCount > 100 {
				return BlobAccessInfo[T]{}, "", status.Errorf(codes.InvalidArgument, "Total number of blocks is %d, which is more than this implementation is willing to support", blockCount)
			}
			blockSectorCount = sectorCount / int64(blockCount)
			if blockSectorCount <= 0 {
				return BlobAccessInfo[T]{}, "", status.Errorf(codes.InvalidArgument, "Block device only has %d sectors (%d bytes each), which is less than the total number of blocks (%d), meaning this backend would be incapable of storing any data", sectorCount, sectorSizeBytes, blockCount)
			}

			blockAllocator = local.NewBlockDeviceBackedBlockAllocator(
				blockDevice,
				sectorSizeBytes,
				blockSectorCount,
				int(blockCount),
				storageTypeName,
			)
		default:
			return BlobAccessInfo[T]{}, "", status.Error(codes.InvalidArgument, "Blocks backend not specified")
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
			persistentStateDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(persistent.StateDirectoryPath))
			if err != nil {
				return BlobAccessInfo[T]{}, "", util.StatusWrapf(err, "Failed to open persistent state directory %#v", persistent.StateDirectoryPath)
			}
			persistentStateStore := local.NewDirectoryBackedPersistentStateStore(persistentStateDirectory)
			persistentState, err := persistentStateStore.ReadPersistentState()
			if err != nil {
				return BlobAccessInfo[T]{}, "", util.StatusWrapf(err, "Failed to reload persistent state from %#v", persistent.StateDirectoryPath)
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
				persistentState.Blocks,
			)
			blockList = persistentBlockList

			// Start goroutines that update the persistent
			// state file when writes and block releases
			// occur.
			if err := persistent.MinimumEpochInterval.CheckValid(); err != nil {
				return BlobAccessInfo[T]{}, "", util.StatusWrap(err, "Failed to obtain minimum epoch duration")
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
				dataSyncer,
			)
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
			int(backend.Local.NewBlocks),
		)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}

		locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
			blockList,
			blockListGrowthPolicy,
			util.DefaultErrorLogger,
			storageTypeName,
			int64(sectorSizeBytes)*blockSectorCount,
			int(backend.Local.OldBlocks),
			int(backend.Local.NewBlocks),
			initialBlockCount,
		)

		keyLocationMap, err := lossymap.NewHashMapFromConfiguration(
			backend.Local.KeyLocationMap,
			"KeyLocationMap:"+storageTypeName,
			local.LocationRecordArrayFactory,
			func(k *lossymap.RecordKey[local.Key]) uint64 {
				h := keyLocationMapHashInitialization
				for _, c := range k.Key {
					h ^= uint64(c)
					h *= 1099511628211
				}
				attempt := k.Attempt
				for i := 0; i < 4; i++ {
					h ^= uint64(attempt & 0xff)
					h *= 1099511628211
					attempt >>= 8
				}
				return h
			},
			func(a, b *local.Location) int {
				if a.BlockIndex < b.BlockIndex {
					return -1
				}
				if a.BlockIndex > b.BlockIndex {
					return 1
				}
				if a.OffsetBytes < b.OffsetBytes {
					return -1
				}
				if a.OffsetBytes > b.OffsetBytes {
					return 1
				}
				return 0
			},
			persistent != nil,
		)
		if err != nil {
			return BlobAccessInfo[T]{}, "", util.StatusWrap(err, "Failed to create key-location map")
		}

		var localBlobAccess blobstore.BlobAccess[T]
		capabilitiesProvider := creator.GetDefaultCapabilitiesProvider()
		chunkingParameters := backend.Local.GetChunkingParameters()
		if chunkingParameters != nil {
			if chunkingParameters.MinChunkSizeBytes < 64 {
				return BlobAccessInfo[T]{}, "", status.Errorf(codes.InvalidArgument, "Chunking parameters must have a minimum chunk size of at least 64 bytes, got %d", chunkingParameters.MinChunkSizeBytes)
			}
			if chunkingParameters.MinChunkSizeBytes > math.MaxInt64 || chunkingParameters.HorizonSizeBytes > math.MaxInt64 {
				return BlobAccessInfo[T]{}, "", status.Error(codes.InvalidArgument, "Chunking parameters could not be represented as int64")
			}
			capabilitiesProvider = capabilities.NewMergingProvider([]capabilities.Provider{
				capabilitiesProvider,
				capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
					CacheCapabilities: &remoteexecution.CacheCapabilities{
						SplitBlobSupport:  true,
						SpliceBlobSupport: true,
						RepMaxCdcParams:   chunkingParameters,
					},
				}),
			})
		}
		if backend.Local.HierarchicalInstanceNames {
			localBlobAccess, err = creator.NewHierarchicalInstanceNamesLocalBlobAccess(
				keyLocationMap,
				locationBlobMap,
				locationBlobMap,
				&globalLock,
				capabilitiesProvider,
			)
			if err != nil {
				return BlobAccessInfo[T]{}, "", err
			}
		} else {
			localBlobAccess = local.NewFlatBlobAccess(
				keyLocationMap,
				locationBlobMap,
				locationBlobMap,
				digestKeyFormat,
				&globalLock,
				storageTypeName,
				capabilitiesProvider,
				creator.GetBinaryCoder(),
			)
		}
		return BlobAccessInfo[T]{
			BlobAccess:      localBlobAccess,
			DigestKeyFormat: digestKeyFormat,
		}, backendType, nil
	case *pb.BlobAccessConfiguration_ReadFallback:
		primary, err := nc.NewNestedBlobAccess(backend.ReadFallback.Primary, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		secondary, err := nc.NewNestedBlobAccess(backend.ReadFallback.Secondary, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		replicator, err := NewBlobReplicatorFromConfiguration(nc.terminationGroup, backend.ReadFallback.Replicator, secondary.BlobAccess, primary, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		return BlobAccessInfo[T]{
			BlobAccess:      readfallback.NewReadFallbackBlobAccess(primary.BlobAccess, secondary.BlobAccess, replicator),
			DigestKeyFormat: primary.DigestKeyFormat.Combine(secondary.DigestKeyFormat),
		}, "read_fallback", nil
	case *pb.BlobAccessConfiguration_Demultiplexing:
		// Construct a trie for each of the backends specified
		// in the configuration indexed by instance name prefix.
		backendsTrie := digest.NewInstanceNameTrie()
		type demultiplexedBackendInfo struct {
			backend             blobstore.BlobAccess[T]
			backendName         string
			instanceNamePatcher digest.InstanceNamePatcher
		}
		backends := make([]demultiplexedBackendInfo, 0, len(backend.Demultiplexing.InstanceNamePrefixes))
		for k, demultiplexed := range backend.Demultiplexing.InstanceNamePrefixes {
			matchInstanceNamePrefix, err := digest.NewInstanceName(k)
			if err != nil {
				return BlobAccessInfo[T]{}, "", util.StatusWrapf(err, "Invalid instance name %#v", k)
			}
			addInstanceNamePrefix, err := digest.NewInstanceName(demultiplexed.AddInstanceNamePrefix)
			if err != nil {
				return BlobAccessInfo[T]{}, "", util.StatusWrapf(err, "Invalid instance name %#v", demultiplexed.AddInstanceNamePrefix)
			}
			backend, err := nc.NewNestedBlobAccess(demultiplexed.Backend, creator)
			if err != nil {
				return BlobAccessInfo[T]{}, "", err
			}
			backendsTrie.Set(matchInstanceNamePrefix, len(backends))
			backends = append(backends, demultiplexedBackendInfo{
				backend:             backend.BlobAccess,
				backendName:         matchInstanceNamePrefix.String(),
				instanceNamePatcher: digest.NewInstanceNamePatcher(matchInstanceNamePrefix, addInstanceNamePrefix),
			})
		}
		return BlobAccessInfo[T]{
			BlobAccess: blobstore.NewDemultiplexingBlobAccess(
				blobstore.NewDemultiplexedBlobAccessGetter(func(i digest.InstanceName) (blobstore.BlobAccess[T], string, digest.InstanceNamePatcher, error) {
					idx := backendsTrie.GetLongestPrefix(i)
					if idx < 0 {
						return nil, "", digest.NoopInstanceNamePatcher, status.Errorf(codes.InvalidArgument, "Unknown instance name: %#v", i.String())
					}
					return backends[idx].backend, backends[idx].backendName, backends[idx].instanceNamePatcher, nil
				}),
			),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "demultiplexing", nil
	case *pb.BlobAccessConfiguration_ReadCanarying:
		config := backend.ReadCanarying
		source, err := nc.NewNestedBlobAccess(config.Source, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		replica, err := nc.NewNestedBlobAccess(config.Replica, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		maximumCacheDuration := config.MaximumCacheDuration
		if err := maximumCacheDuration.CheckValid(); err != nil {
			return BlobAccessInfo[T]{}, "", util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid maximum cache duration")
		}
		return BlobAccessInfo[T]{
			BlobAccess: blobstore.NewReadCanaryingBlobAccess(
				source.BlobAccess,
				replica.BlobAccess,
				clock.SystemClock,
				eviction.NewMetricsSet(eviction.NewLRUSet[string](), "ReadCanaryingBlobAccess"),
				int(config.MaximumCacheSize),
				maximumCacheDuration.AsDuration(),
				util.DefaultErrorLogger,
			),
			DigestKeyFormat: source.DigestKeyFormat.Combine(replica.DigestKeyFormat),
		}, "read_canarying", nil
	case *pb.BlobAccessConfiguration_ZipReading:
		config := backend.ZipReading
		file, err := os.Open(config.Path)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		fileInfo, err := file.Stat()
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		zipReader, err := zip.NewReader(file, fileInfo.Size())
		if err != nil {
			file.Close()
			return BlobAccessInfo[T]{}, "", util.StatusWrapf(err, "Failed to open ZIP file %#v", config.Path)
		}

		digestKeyFormat := creator.GetBaseDigestKeyFormat()

		return BlobAccessInfo[T]{
			BlobAccess: blobstore.NewZIPReadingBlobAccess(
				creator.GetDefaultCapabilitiesProvider(),
				creator.GetBinaryCoder(),
				digestKeyFormat,
				zipReader.File,
			),
			DigestKeyFormat: digestKeyFormat,
		}, "zip_reading", nil
	case *pb.BlobAccessConfiguration_ZipWriting:
		config := backend.ZipWriting
		zipPath := config.Path
		file, err := os.OpenFile(zipPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}
		digestKeyFormat := creator.GetBaseDigestKeyFormat()

		blobAccess := blobstore.NewZIPWritingBlobAccess(
			creator.GetDefaultCapabilitiesProvider(),
			creator.GetBinaryCoder(),
			digestKeyFormat,
			file,
		)

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

		return BlobAccessInfo[T]{
			BlobAccess:      blobAccess,
			DigestKeyFormat: digestKeyFormat,
		}, "zip_writing", nil
	case *pb.BlobAccessConfiguration_DeadlineEnforcing:
		base, err := nc.NewNestedBlobAccess(backend.DeadlineEnforcing.Backend, creator)
		if err != nil {
			return BlobAccessInfo[T]{}, "", err
		}

		timeout := backend.DeadlineEnforcing.Timeout
		if err := timeout.CheckValid(); err != nil {
			return BlobAccessInfo[T]{}, "", util.StatusWrap(err, "Invalid timeout for deadline enforcement")
		}

		return BlobAccessInfo[T]{
			BlobAccess:      blobstore.NewDeadlineEnforcingBlobAccess(base.BlobAccess, timeout.AsDuration()),
			DigestKeyFormat: base.DigestKeyFormat,
		}, "deadline_enforcing", nil
	}
	return creator.NewCustomBlobAccess(nc.terminationGroup, configuration, nc)
}

// NewNestedBlobAccess may be called by
// BlobAccessCreator.NewCustomBlobAccess() to create BlobAccess
// objects for instances nested inside the configuration.
func (nc *simpleNestedBlobAccessCreator[T]) NewNestedBlobAccess(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator[T]) (BlobAccessInfo[T], error) {
	if configuration == nil {
		return BlobAccessInfo[T]{}, status.Error(codes.InvalidArgument, "Storage configuration not specified")
	}

	// Protobuf does not support anchors/aliases like YAML. Have
	// separate 'with_labels' and 'labels' backends that can be used
	// to declare anchors and aliases, respectively.
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_WithLabels:
		config := backend.WithLabels

		// Inherit labels from the parent.
		labels := map[string]BlobAccessInfo[T]{}
		for label, labelBackend := range nc.labels {
			labels[label] = labelBackend
		}

		// Add additional labels declared in config.
		for label, labelBackend := range config.Labels {
			if _, ok := labels[label]; ok {
				// Disallow shadowing.
				return BlobAccessInfo[T]{}, status.Errorf(codes.InvalidArgument, "Label %#v has already been declared", label)
			}
			info, err := nc.NewNestedBlobAccess(labelBackend, creator)
			if err != nil {
				return BlobAccessInfo[T]{}, util.StatusWrapf(err, "Label %#v", label)
			}
			labels[label] = info
		}

		return (&simpleNestedBlobAccessCreator[T]{
			terminationGroup: nc.terminationGroup,
			labels:           labels,
		}).NewNestedBlobAccess(config.Backend, creator)
	case *pb.BlobAccessConfiguration_Label:
		if labelBackend, ok := nc.labels[backend.Label]; ok {
			return labelBackend, nil
		}
		return BlobAccessInfo[T]{}, status.Errorf(codes.InvalidArgument, "Label %#v not declared", backend.Label)
	}

	backend, backendType, err := nc.newNestedBlobAccessBare(configuration, creator)
	if err != nil {
		return BlobAccessInfo[T]{}, err
	}
	return BlobAccessInfo[T]{
		BlobAccess:      blobstore.NewMetricsBlobAccess(backend.BlobAccess, clock.SystemClock, creator.GetStorageTypeName(), backendType),
		DigestKeyFormat: backend.DigestKeyFormat,
	}, nil
}

// NewBlobAccessFromConfiguration creates a BlobAccess object based on a
// configuration file.
func NewBlobAccessFromConfiguration[T any](terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator[T]) (BlobAccessInfo[T], error) {
	nestedCreator := &simpleNestedBlobAccessCreator[T]{
		terminationGroup: terminationGroup,
	}
	backend, err := nestedCreator.NewNestedBlobAccess(configuration, creator)
	if err != nil {
		return BlobAccessInfo[T]{}, err
	}
	return BlobAccessInfo[T]{
		BlobAccess:      creator.WrapTopLevelBlobAccess(backend.BlobAccess),
		DigestKeyFormat: backend.DigestKeyFormat,
	}, nil
}

// NewCASAndACFromConfiguration is a convenience function to create the
// constituent parts of a Content Addressable Storage (CAS) and a
// BlobAccess for the Action Cache. Most Buildbarn components tend to
// require access to both these data stores.
func NewCASAndACFromConfiguration(terminationGroup program.Group, configuration *pb.BlobstoreConfiguration, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool bb_zstd.Pool) (reader.Reader[[]byte], blobstore.BlobAccess[*chunk.Chunk], blobstore.BlobAccess[chunk.Mapping], chunk.MappingFetcher, capabilities.CDCParametersFetcher, digest.KeyFormat, blobstore.BlobAccess[*remoteexecution.ActionResult], error) {
	chunkBytesReader, chunkStorage, chunkMappingStorage, chunkMappingFetcher, cdcParametersFetcher, digestKeyFormat, err := NewCASFromConfiguration(terminationGroup, configuration.ContentAddressableStorage, grpcClientFactory, maximumMessageSizeBytes, zstdPool)
	if err != nil {
		return nil, nil, nil, nil, nil, digest.KeyWithoutInstance, nil, util.StatusWrap(err, "Failed to create Content Addressable Storage")
	}

	actionCache, err := NewBlobAccessFromConfiguration(
		terminationGroup,
		configuration.GetActionCache(),
		NewACBlobAccessCreator(
			chunkBytesReader,
			chunkStorage,
			chunkMappingStorage,
			chunkMappingFetcher,
			cdcParametersFetcher,
			digestKeyFormat,
			grpcClientFactory,
			maximumMessageSizeBytes,
		),
	)
	if err != nil {
		return nil, nil, nil, nil, nil, digest.KeyWithoutInstance, nil, util.StatusWrap(err, "Failed to create Action Cache")
	}

	return chunkBytesReader, chunkStorage, chunkMappingStorage, chunkMappingFetcher, cdcParametersFetcher, digestKeyFormat, actionCache.BlobAccess, nil
}

// NewCASFromConfiguration is a convenience function to create the
// constituent parts of a Content Addressable Storage (CAS) from
// configuration.
func NewCASFromConfiguration(terminationGroup program.Group, configuration *pb.ContentAddressableStorageConfiguration, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool bb_zstd.Pool) (reader.Reader[[]byte], blobstore.BlobAccess[*chunk.Chunk], blobstore.BlobAccess[chunk.Mapping], chunk.MappingFetcher, capabilities.CDCParametersFetcher, digest.KeyFormat, error) {
	chunkStorageInfo, err := NewBlobAccessFromConfiguration(
		terminationGroup,
		configuration.GetChunkStorage(),
		NewCSBlobAccessCreator(grpcClientFactory, maximumMessageSizeBytes, zstdPool),
	)
	if err != nil {
		return nil, nil, nil, nil, nil, digest.KeyWithoutInstance, util.StatusWrap(err, "Failed to create Chunk Storage")
	}
	chunkStorage := chunkStorageInfo.BlobAccess

	chunkMappingStorageInfo, err := NewBlobAccessFromConfiguration(
		terminationGroup,
		configuration.GetChunkMappingStorage(),
		NewCMSBlobAccessCreator(&chunkStorageInfo, grpcClientFactory, maximumMessageSizeBytes, zstdPool),
	)
	if err != nil {
		return nil, nil, nil, nil, nil, digest.KeyWithoutInstance, util.StatusWrap(err, "Failed to create Chunk Mapping Storage")
	}
	chunkMappingStorage := chunkMappingStorageInfo.BlobAccess
	var chunkMappingFetcher chunk.MappingFetcher = blobstore.NewBlobAccessMappingFetcher(chunkMappingStorage)
	if configuration.GetChunkMappingCache() != nil {
		cache, err := ttlcache.NewTTLCacheFromConfiguration[digest.Digest, chunk.Mapping](
			configuration.ChunkMappingCache,
			clock.SystemClock,
			"ChunkMappingCache",
		)
		if err != nil {
			return nil, nil, nil, nil, nil, digest.KeyWithoutInstance, util.StatusWrap(err, "Failed to create chunk mapping cache")
		}
		chunkMappingFetcher = chunk.NewCachingMappingFetcher(chunkMappingFetcher, cache)
	}

	// The chunking parameters are a property of the Chunk Storage
	// (CS), so the CDC parameters are fetched from there.
	cdcParametersFetcher := capabilities.NewCDCParametersFetcher(chunkStorage)
	if configuration.GetCdcParameterCache() != nil {
		cache, err := ttlcache.NewTTLCacheFromConfiguration[digest.InstanceName, *remoteexecution.RepMaxCdcParams](
			configuration.CdcParameterCache,
			clock.SystemClock,
			"CDCParameterCache",
		)
		if err != nil {
			return nil, nil, nil, nil, nil, digest.KeyWithoutInstance, util.StatusWrap(err, "Failed to create cdc parameter cache")
		}
		cdcParametersFetcher = capabilities.NewCachingCDCParametersFetcher(cdcParametersFetcher, cache)
	}

	var chunkBytesReader reader.Reader[[]byte] = cas.NewChunkBytesReader(chunkStorage)
	if chunkCacheConfiguration := configuration.GetChunkCache(); chunkCacheConfiguration != nil {
		cache, err := ttlcache.NewTTLCacheFromConfiguration[digest.Digest, []byte](
			chunkCacheConfiguration,
			clock.SystemClock,
			"ChunkCache",
		)
		if err != nil {
			return nil, nil, nil, nil, nil, digest.KeyWithoutInstance, util.StatusWrap(err, "Failed to create chunk cache")
		}
		chunkBytesReader = reader.NewCachingReader(chunkBytesReader, cache)
	}

	return chunkBytesReader, chunkStorage, chunkMappingStorage, chunkMappingFetcher, cdcParametersFetcher, chunkStorageInfo.DigestKeyFormat.Combine(chunkMappingStorageInfo.DigestKeyFormat), nil
}
