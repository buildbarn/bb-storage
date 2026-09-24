package concurrencytest_test

import (
	"sync"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
)

// TestPersistentBlockListConcurrentFinalizeAndResolve drives many
// concurrent BlockListPutFinalizer calls together with concurrent
// BlockReferenceResolver reads and a sync cycle. Without internal
// synchronisation in PersistentBlockList this trips the race detector
// two ways: the per-block writtenOffsetBytes update races between two
// finalizers, and the finalizer's epochHashSeeds append races with the
// resolver's read of the same slice.
//
// The sync goroutine is load-bearing, not decoration. The append branch
// fires only when len(epochLastAbsoluteBlockIndex) == synchronizingEpochs
// or the current epoch predates the written block. With a single block
// and no sync cycle, neither is ever true again after the seed put, so
// epochHashSeeds stays frozen at one element and the append/read race
// never occurs -- the test would silently cover only half of what it
// claims. NotifySyncStarting() republishes synchronizingEpochs, which
// re-arms the first clause. The epoch-growth assertion at the bottom is
// what stops that regressing.
//
// Run under the race detector as:
//
//	bazel test //pkg/blobstore/local/concurrencytest:concurrencytest_test \
//	    --host_platform=@platforms//host \
//	    --@rules_go//go/config:race \
//	    --test_filter=TestPersistentBlockListConcurrentFinalizeAndResolve
//
// Plain `go test` does not work in this repository: pkg/digest requires a
// protobuf symbol that bazel regenerates from .proto but that is absent
// from the checked-in Go bindings of github.com/bazelbuild/remote-apis.
func TestPersistentBlockListConcurrentFinalizeAndResolve(t *testing.T) {
	const (
		blockSize    = 1 << 20 // 1 MiB per block
		blobSize     = 256
		blobsPerProd = 60 // 32*60*256 = 480KB, fits in one block
		producers    = 32
		readers      = 16
	)

	blockAllocator := local.NewInMemoryBlockAllocator(blockSize)
	blockList, _ := local.NewPersistentBlockList(blockAllocator, 0, nil)

	// Seed a single block so producers have somewhere to put.
	if err := blockList.PushBack(); err != nil {
		t.Fatalf("seed PushBack: %v", err)
	}

	// Seed an epoch with a single put. This makes the resolver
	// slices non-empty so readers can call them validly. Real
	// callers never invoke the resolver on an empty BlockList
	// because the KeyLocationMap.Get would return NotFound first.
	{
		writer := blockList.Put(0, blobSize)
		finalizer := writer(buffer.NewValidatedBufferFromByteSlice(make([]byte, blobSize)))
		if _, err := finalizer(); err != nil {
			t.Fatalf("seed finalizer: %v", err)
		}
	}

	epochsBefore := synchronizedEpochs(blockList)

	// Syncer: models PeriodicSyncer's epoch boundaries, which is what
	// makes finalizers take the epochHashSeeds append branch. Both
	// calls take bl.mu exclusively, so this also exercises finalizers
	// and resolver reads racing against the lifecycle methods.
	syncerStop := make(chan struct{})
	var syncerWg sync.WaitGroup
	syncerWg.Add(1)
	go func() {
		defer syncerWg.Done()
		for {
			select {
			case <-syncerStop:
				return
			default:
			}
			blockList.NotifySyncStarting(false)
			blockList.NotifySyncCompleted()
			time.Sleep(time.Millisecond)
		}
	}()

	// Producers: take a slot, allocate, copy, finalize. Each
	// producer races with peers on writtenOffsetBytes and, thanks to
	// the syncer above, genuinely reaches the epochHashSeeds append.
	var producersWg sync.WaitGroup
	for p := 0; p < producers; p++ {
		producersWg.Add(1)
		go func() {
			defer producersWg.Done()
			data := make([]byte, blobSize)
			for i := 0; i < blobsPerProd; i++ {
				writer := blockList.Put(0, blobSize)
				finalizer := writer(buffer.NewValidatedBufferFromByteSlice(data))
				if _, err := finalizer(); err != nil {
					t.Errorf("finalizer: %v", err)
					return
				}
			}
		}()
	}

	// Readers: hammer the resolver methods. Use a fresh
	// BlockReference each time so they hit different slots.
	stop := make(chan struct{})
	var readersWg sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWg.Add(1)
		go func() {
			defer readersWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// BlockReferenceToBlockIndex reads
				// epochHashSeeds[epochIndex]; concurrent
				// slice append by the finalizer's append
				// branch is the documented race.
				blockList.BlockReferenceToBlockIndex(local.BlockReference{
					EpochID:        0,
					BlocksFromLast: 0,
				})
				// BlockIndexToBlockReference reads the
				// last entry of the same slice.
				blockList.BlockIndexToBlockReference(0)
			}
		}()
	}

	producersWg.Wait()
	close(syncerStop)
	syncerWg.Wait()
	close(stop)
	readersWg.Wait()

	// Prove the append branch was actually reached. Without this the
	// test can pass while covering only the writtenOffsetBytes race.
	if epochsAfter := synchronizedEpochs(blockList); epochsAfter <= epochsBefore {
		t.Errorf("synchronized epochs did not grow (%d -> %d): the finalizer never "+
			"took the epochHashSeeds append branch, so the append/read race was "+
			"never exercised", epochsBefore, epochsAfter)
	}
}

// synchronizedEpochs counts the epochs PersistentBlockList considers
// durable, which is how many times the finalizer has taken the
// epochHashSeeds append branch and had it survive a sync cycle.
func synchronizedEpochs(blockList *local.PersistentBlockList) int {
	_, blocks := blockList.GetPersistentState()
	n := 0
	for _, b := range blocks {
		n += len(b.EpochHashSeeds)
	}
	return n
}
