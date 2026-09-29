package concurrencytest_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// TestFlatBlobAccessConcurrentMixedTrafficVolatile is the
// volatileBlockList counterpart to
// TestFlatBlobAccessConcurrentMixedTraffic.
//
// It exists because relaxing FlatBlobAccess.finalizePut from the outer
// write lock to the outer read lock changed the contract for *every*
// BlockList implementation, not just PersistentBlockList.
// PersistentBlockList was given a private mutex to compensate;
// volatileBlockList was not, and its safety rests entirely on the
// argument that its BlockListPutFinalizer is a pass-through to the
// Block and therefore mutates no shared state. That argument is
// correct for both shipped BlockAllocators, but it was asserted rather
// than tested, and the other concurrency tests here all construct a
// PersistentBlockList -- so nothing exercised it.
//
// This matters beyond tidiness: a deployment whose key-location map is
// in memory cannot enable persistency (see LocalBlobAccessConfiguration
// 'persistent'), so it runs on volatileBlockList. For such a
// deployment this is the hot path, and it was the one path with no
// coverage.
//
// Run under the race detector as:
//
//	bazel test //pkg/blobstore/local/concurrencytest:concurrencytest_test \
//	    --host_platform=@platforms//host \
//	    --@rules_go//go/config:race \
//	    --test_filter=TestFlatBlobAccessConcurrentMixedTrafficVolatile
func TestFlatBlobAccessConcurrentMixedTrafficVolatile(t *testing.T) {
	const (
		blobSize       = 256
		blockSizeBytes = 1 << 20 // 1 MiB
		oldBlocks      = 2
		currentBlocks  = 6
		newBlocks      = 2
		klmEntries     = 16384
		duration       = 2 * time.Second
	)

	blockAllocator := local.NewInMemoryBlockAllocator(int(blockSizeBytes))
	// The distinction under test: no PersistentBlockList, hence no
	// internal mutex backing the finalizer.
	blockList := local.NewVolatileBlockList(blockAllocator)
	growthPolicy := local.NewImmutableBlockListGrowthPolicy(currentBlocks, newBlocks)

	locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
		blockList,
		growthPolicy,
		silentErrorLogger{},
		"volatiletest",
		blockSizeBytes,
		oldBlocks,
		newBlocks,
		0,
	)

	recordsCount := klmEntries
	for !isPrime(recordsCount) {
		recordsCount++
	}
	recordArray := local.NewInMemoryLocationRecordArray(recordsCount, locationBlobMap)
	keyLocationMap := local.NewHashingKeyLocationMap(
		recordArray,
		recordsCount,
		0xfeedface,
		16,
		64,
		"volatiletest",
	)

	var globalLock sync.RWMutex
	access := local.NewFlatBlobAccess(
		keyLocationMap,
		locationBlobMap,
		digest.KeyWithoutInstance,
		&globalLock,
		0,
		"volatiletest",
		capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
	)

	ctx := context.Background()

	populated := make([]digest.Digest, 256)
	for i := range populated {
		d := makeDigest(uint64(i), blobSize)
		populated[i] = d
		if err := access.Put(ctx, d, buffer.NewValidatedBufferFromByteSlice(make([]byte, blobSize))); err != nil {
			t.Fatalf("populate Put[%d]: %v", i, err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var puts, gets, getsMissing, finds atomic.Uint64

	const writers, readers, finders = 8, 8, 4

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			seq := uint64(1_000_000) + uint64(id)*1_000_000
			data := make([]byte, blobSize)
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq++
				if err := access.Put(ctx, makeDigest(seq, blobSize), buffer.NewValidatedBufferFromByteSlice(data)); err == nil {
					puts.Add(1)
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(uint64(id), 4242))
			for {
				select {
				case <-stop:
					return
				default:
				}
				buf := access.Get(ctx, populated[rnd.IntN(len(populated))])
				if _, err := buf.ToByteSlice(blobSize); err != nil {
					getsMissing.Add(1)
				} else {
					gets.Add(1)
				}
			}
		}(r)
	}

	for f := 0; f < finders; f++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(uint64(id), 1717))
			for {
				select {
				case <-stop:
					return
				default:
				}
				b := digest.NewSetBuilder(32)
				for i := 0; i < 32; i++ {
					b.Add(populated[rnd.IntN(len(populated))])
				}
				if _, err := access.FindMissing(ctx, b.Build()); err == nil {
					finds.Add(1)
				}
			}
		}(f)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// gets must be non-zero on its own: getsMissing counts FAILED reads,
	// so a guard on the sum would accept a run in which every read
	// errored and the read path was never really exercised.
	if puts.Load() == 0 || gets.Load() == 0 || finds.Load() == 0 {
		t.Errorf("workload did not exercise all paths: puts=%d gets=%d gets_missing=%d finds=%d",
			puts.Load(), gets.Load(), getsMissing.Load(), finds.Load())
	}
}
