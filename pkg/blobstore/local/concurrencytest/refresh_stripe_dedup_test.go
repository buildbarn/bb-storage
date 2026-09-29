package concurrencytest_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// refreshCopyDelay models the cost of copying a blob from an old block
// into a new one. Without it every refresh completes so quickly that
// concurrent finders never overlap and the test cannot observe
// duplicate refreshes at all.
const refreshCopyDelay = 500 * time.Microsecond

// stubKeyLocationMap is a trivially correct KeyLocationMap. It is
// internally synchronised so that this test measures the refresh
// dedup behaviour of FlatBlobAccess rather than the hash-table
// behaviour of hashingKeyLocationMap.
type stubKeyLocationMap struct {
	lock sync.Mutex
	m    map[local.Key]local.Location
}

func (klm *stubKeyLocationMap) Get(key local.Key) (local.Location, error) {
	klm.lock.Lock()
	defer klm.lock.Unlock()
	if location, ok := klm.m[key]; ok {
		return location, nil
	}
	return local.Location{}, status.Error(codes.NotFound, "Blob not found")
}

func (klm *stubKeyLocationMap) Put(key local.Key, location local.Location) error {
	klm.lock.Lock()
	defer klm.lock.Unlock()
	klm.m[key] = location
	return nil
}

// countingLocationBlobMap models the "old" -> "new" migration that
// drives refresh. Blobs in block 0 are old and need refreshing; blobs
// in block 1 are new and do not. Every refresh allocation is counted,
// which is what lets the test observe duplicate refreshes.
type countingLocationBlobMap struct {
	// Index of the first block that does not need refreshing. Blobs
	// below it are old, and Put() moves them here. Keeping it on the
	// fixture rather than hardcoding it keeps the seeding, the refresh
	// decision and the final assertion in step.
	newBlockIndex int

	lock        sync.Mutex
	nextOffset  int64
	allocations atomic.Int64
}

func (lbm *countingLocationBlobMap) Get(location local.Location) (local.LocationBlobGetter, bool) {
	needsRefresh := location.BlockIndex < lbm.newBlockIndex
	return func(digest.Digest) buffer.Buffer {
		return buffer.NewValidatedBufferFromByteSlice(make([]byte, location.SizeBytes))
	}, needsRefresh
}

func (lbm *countingLocationBlobMap) Put(sizeBytes int64) (local.LocationBlobPutWriter, error) {
	lbm.allocations.Add(1)

	lbm.lock.Lock()
	offsetBytes := lbm.nextOffset
	lbm.nextOffset += sizeBytes
	lbm.lock.Unlock()

	return func(b buffer.Buffer) local.LocationBlobPutFinalizer {
		b.Discard()
		// FlatBlobAccess deliberately runs the copy with no lock held,
		// so a real refresh takes roughly a disk read. Model that: it
		// is the window in which a second caller can observe the blob
		// as still-old and start a duplicate refresh.
		time.Sleep(refreshCopyDelay)
		return func() (local.Location, error) {
			return local.Location{
				BlockIndex:  lbm.newBlockIndex,
				OffsetBytes: offsetBytes,
				SizeBytes:   sizeBytes,
			}, nil
		}
	}, nil
}

// TestFindMissingRefreshDedup pins the property that commit "Stripe
// refreshLock 256-way by digest" must preserve: concurrent
// FindMissing() calls over the same digests must refresh each blob
// exactly once.
//
// The striped refresh lock replaced a single global refreshLock. The
// global lock made dedup trivial by single-threading all refresh; the
// striped version must still guarantee it, because a duplicate refresh
// doubles the write traffic for a blob for no benefit. A caller that
// loses the race for a stripe must observe the winner's committed
// key-location entry and skip.
//
// Without any refresh serialisation this assertion fails with
// allocations > blobCount. It also fails if the stripe were released
// before finalizePut committed the new location.
//
// Run under the race detector as:
//
//	bazel test //pkg/blobstore/local/concurrencytest:concurrencytest_test \
//	    --host_platform=@platforms//host \
//	    --@rules_go//go/config:race \
//	    --test_filter=TestFindMissingRefreshDedup
func TestFindMissingRefreshDedup(t *testing.T) {
	const (
		blobCount     = 128
		blobSizeBytes = 256
		finders       = 16
	)

	keyLocationMap := &stubKeyLocationMap{m: map[local.Key]local.Location{}}
	locationBlobMap := &countingLocationBlobMap{newBlockIndex: 1}

	// Seed every blob into block 0, i.e. "old" and in need of refresh.
	digests := make([]digest.Digest, 0, blobCount)
	setBuilder := digest.NewSetBuilder(blobCount)
	for i := 0; i < blobCount; i++ {
		d := makeDigest(uint64(i), blobSizeBytes)
		digests = append(digests, d)
		setBuilder.Add(d)
		keyLocationMap.m[local.NewKeyFromString(d.GetKey(digest.KeyWithoutInstance))] = local.Location{
			BlockIndex:  0,
			OffsetBytes: int64(i) * blobSizeBytes,
			SizeBytes:   blobSizeBytes,
		}
	}
	digestSet := setBuilder.Build()

	var globalLock sync.RWMutex
	access := local.NewFlatBlobAccess(
		keyLocationMap,
		locationBlobMap,
		digest.KeyWithoutInstance,
		&globalLock,
		0,
		"dedup",
		capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
	)

	// Every finder races to refresh the same set of blobs.
	ctx := context.Background()
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, finders)
	missingCounts := make([]int, finders)
	for f := 0; f < finders; f++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			missing, err := access.FindMissing(ctx, digestSet)
			errs[id] = err
			missingCounts[id] = missing.Length()
		}(f)
	}
	close(start)
	wg.Wait()

	for id, err := range errs {
		if err != nil {
			t.Fatalf("finder %d: FindMissing: %v", id, err)
		}
		if missingCounts[id] != 0 {
			t.Errorf("finder %d: reported %d missing blobs, want 0 (all were present)", id, missingCounts[id])
		}
	}

	if got := locationBlobMap.allocations.Load(); got != blobCount {
		t.Errorf("refresh allocations = %d, want exactly %d (one per blob); "+
			"a higher count means concurrent FindMissing calls duplicated refresh work",
			got, blobCount)
	}

	// Every blob must now live in the "new" block.
	for i, d := range digests {
		location, err := keyLocationMap.Get(local.NewKeyFromString(d.GetKey(digest.KeyWithoutInstance)))
		if err != nil {
			t.Fatalf("blob %d: %v", i, err)
		}
		if location.BlockIndex != locationBlobMap.newBlockIndex {
			t.Errorf("blob %d: BlockIndex = %d, want %d (refreshed)",
				i, location.BlockIndex, locationBlobMap.newBlockIndex)
		}
	}
}
