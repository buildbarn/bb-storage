package concurrencytest_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// trackingLocationBlobMap is countingLocationBlobMap plus a record of
// how many refresh copies were in flight simultaneously. The copy is
// exactly the region the refresh semaphore is meant to bound, so the
// high-water mark is the property under test.
type trackingLocationBlobMap struct {
	// See countingLocationBlobMap.newBlockIndex.
	newBlockIndex int

	lock       sync.Mutex
	nextOffset int64

	inFlight     atomic.Int64
	peakInFlight atomic.Int64

	copyStarted chan<- struct{}
	copyRelease <-chan struct{}
}

func (lbm *trackingLocationBlobMap) Get(location local.Location) (local.LocationBlobGetter, bool) {
	needsRefresh := location.BlockIndex < lbm.newBlockIndex
	return func(digest.Digest) ([]byte, local.IntegrityErrorCallback, error) {
		return make([]byte, location.SizeBytes), func() {}, nil
	}, needsRefresh
}

func (lbm *trackingLocationBlobMap) Put(sizeBytes int64) (local.LocationBlobPutWriter, error) {
	lbm.lock.Lock()
	offsetBytes := lbm.nextOffset
	lbm.nextOffset += sizeBytes
	lbm.lock.Unlock()

	return func(data []byte) local.LocationBlobPutFinalizer {
		// Model the copy, and record concurrency across it.
		current := lbm.inFlight.Add(1)
		for {
			peak := lbm.peakInFlight.Load()
			if current <= peak || lbm.peakInFlight.CompareAndSwap(peak, current) {
				break
			}
		}
		if lbm.copyStarted != nil {
			lbm.copyStarted <- struct{}{}
			<-lbm.copyRelease
		} else {
			time.Sleep(refreshCopyDelay)
		}
		lbm.inFlight.Add(-1)

		return func() (local.Location, error) {
			return local.Location{
				BlockIndex:  lbm.newBlockIndex,
				OffsetBytes: offsetBytes,
				SizeBytes:   sizeBytes,
			}, nil
		}
	}, nil
}

// TestFindMissingRefreshConcurrencyLimit pins the bandwidth property
// that the striped refresh lock would otherwise have dropped.
//
// Upstream's single refreshLock served two purposes: deduplicating
// refreshes of the same blob, and limiting refresh bandwidth to one
// thread. Striping preserves the first (see TestFindMissingRefreshDedup)
// but not the second, so FlatBlobAccess carries an explicit semaphore.
// This test asserts that the semaphore is actually enforced, and that
// it is not so conservative that refresh is still serialised.
//
// Each finder refreshes a disjoint set of blobs, so no refresh is
// deduplicated away and every finder does real copy work. Without the
// semaphore the observed peak rises to the number of finders.
func TestFindMissingRefreshConcurrencyLimit(t *testing.T) {
	const (
		finders        = 16
		blobsPerFinder = 8
		blobSizeBytes  = 256
	)

	for _, tc := range []struct {
		name               string
		refreshConcurrency int
		wantPeakAtMost     int64
		wantPeakAtLeast    int64
	}{
		{
			// The historical behaviour, now opt-in rather than
			// mandatory: refresh is fully serialised.
			name:               "SingleThreaded",
			refreshConcurrency: 1,
			wantPeakAtMost:     1,
			wantPeakAtLeast:    1,
		},
		{
			// The limit must bind: 16 finders are pushing, so
			// without the semaphore the peak would exceed 4.
			name:               "Bounded",
			refreshConcurrency: 4,
			wantPeakAtMost:     4,
			wantPeakAtLeast:    2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyLocationMap := &stubKeyLocationMap{m: map[local.Key]local.Location{}}
			locationBlobMap := &trackingLocationBlobMap{newBlockIndex: 1}

			// Seed every blob into block 0, i.e. in need of refresh,
			// and give each finder its own disjoint digest set.
			digestSets := make([]digest.Set, 0, finders)
			for f := 0; f < finders; f++ {
				setBuilder := digest.NewSetBuilder(blobsPerFinder)
				for i := 0; i < blobsPerFinder; i++ {
					d := makeDigest(uint64(f*blobsPerFinder+i), blobSizeBytes)
					setBuilder.Add(d)
					keyLocationMap.m[local.NewKeyFromString(d.GetKey(digest.KeyWithoutInstance))] = local.Location{
						BlockIndex:  0,
						OffsetBytes: int64(i) * blobSizeBytes,
						SizeBytes:   blobSizeBytes,
					}
				}
				digestSets = append(digestSets, setBuilder.Build())
			}

			var globalLock sync.RWMutex
			access, err := local.NewFlatBlobAccess(
				keyLocationMap,
				nil,
				locationBlobMap,
				digest.KeyWithoutInstance,
				&globalLock,
				tc.refreshConcurrency,
				"limit",
				capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
				byteCoder{},
			)
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()
			start := make(chan struct{})
			var wg sync.WaitGroup
			errs := make([]error, finders)
			for f := 0; f < finders; f++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					<-start
					_, errs[id] = access.FindMissing(ctx, digestSets[id])
				}(f)
			}
			close(start)
			wg.Wait()

			for id, err := range errs {
				if err != nil {
					t.Fatalf("finder %d: FindMissing: %v", id, err)
				}
			}

			peak := locationBlobMap.peakInFlight.Load()
			if peak > tc.wantPeakAtMost {
				t.Errorf("peak concurrent refreshes = %d, want at most %d; "+
					"the refresh semaphore is not bounding the copy",
					peak, tc.wantPeakAtMost)
			}
			if peak < tc.wantPeakAtLeast {
				t.Errorf("peak concurrent refreshes = %d, want at least %d; "+
					"refresh is more serialised than configured",
					peak, tc.wantPeakAtLeast)
			}
			if remaining := locationBlobMap.inFlight.Load(); remaining != 0 {
				t.Errorf("in-flight refreshes = %d after all finders returned, want 0", remaining)
			}
		})
	}
}

// TestRefreshConcurrencyRespectsContext keeps the only slot occupied
// until a second caller, already blocked waiting for that slot, is
// canceled. synctest.Wait() guarantees the second caller is durably
// blocked before cancel() is called, so this exercises the wakeup of a
// queued caller rather than the early check of an already-canceled
// context.
func TestRefreshConcurrencyRespectsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const blobSizeBytes = 256
		keyLocationMap := &stubKeyLocationMap{m: map[local.Key]local.Location{}}
		copyStarted := make(chan struct{}, 2)
		copyRelease := make(chan struct{})
		release := sync.OnceFunc(func() { close(copyRelease) })
		defer release()
		locationBlobMap := &trackingLocationBlobMap{
			newBlockIndex: 1,
			copyStarted:   copyStarted,
			copyRelease:   copyRelease,
		}

		first := makeDigest(0, blobSizeBytes)
		firstKey := local.NewKeyFromString(first.GetKey(digest.KeyWithoutInstance))
		second := makeDigest(1, blobSizeBytes)
		for seq := uint64(2); local.NewKeyFromString(second.GetKey(digest.KeyWithoutInstance))[0] == firstKey[0]; seq++ {
			second = makeDigest(seq, blobSizeBytes)
		}
		// Different stripes ensure the second caller waits on the slot,
		// not on the mutex held by the first caller.
		for _, d := range []digest.Digest{first, second} {
			keyLocationMap.m[local.NewKeyFromString(d.GetKey(digest.KeyWithoutInstance))] = local.Location{SizeBytes: blobSizeBytes}
		}

		var globalLock sync.RWMutex
		access, err := local.NewFlatBlobAccess(keyLocationMap, nil, locationBlobMap,
			digest.KeyWithoutInstance, &globalLock, 1, "cancel",
			capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}), byteCoder{})
		if err != nil {
			t.Fatal(err)
		}
		firstDone := make(chan error, 1)
		go func() {
			_, err := access.FindMissing(context.Background(), first.ToSingletonSet())
			firstDone <- err
		}()
		<-copyStarted

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		secondDone := make(chan error, 1)
		go func() {
			_, err := access.FindMissing(ctx, second.ToSingletonSet())
			secondDone <- err
		}()
		synctest.Wait()
		select {
		case err := <-secondDone:
			t.Fatalf("second refresh returned %v while the only slot was occupied", err)
		default:
		}

		cancel()
		select {
		case err := <-secondDone:
			if status.Code(err) != codes.Canceled {
				t.Fatalf("second refresh: got %v, want Canceled", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("cancellation waited for the occupied refresh slot")
		}

		release()
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
		// Cancellation must not leak a stripe or slot: the second blob
		// still needs refreshing and should succeed with a live context.
		if _, err := access.FindMissing(context.Background(), second.ToSingletonSet()); err != nil {
			t.Fatal(err)
		}
	})
}
