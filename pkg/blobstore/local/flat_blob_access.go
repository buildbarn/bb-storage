package local

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// refreshStripeCount is the number of per-digest stripes used to
// serialise refresh operations. Stripe index is the first byte of the
// SHA-256 Key, which is uniformly distributed, so 256 stripes give a
// 1:1 mapping with no hash function needed.
const refreshStripeCount = 256

// DefaultRefreshConcurrency is the number of refreshes of distinct
// blobs that may run concurrently when no explicit limit is configured.
//
// The historical value was effectively 1: a single refreshLock
// serialised every refresh in the process, which made refresh latency
// scale with (concurrent callers x blobs per call x copy time) and
// could stall the read path for seconds. Bounding concurrency at 64
// removes that serialisation while still capping how much read and
// write bandwidth refreshing may consume at once.
const DefaultRefreshConcurrency = 64

var (
	flatBlobAccessPrometheusMetrics sync.Once

	flatBlobAccessRefreshesBlobs = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "flat_blob_access_refreshes_blobs",
			Help:      "The number of blobs that were refreshed when requested",
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1.0, 2.0, 16)...),
		},
		[]string{"storage_type", "operation"},
	)

	flatBlobAccessRefreshesDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "flat_blob_access_refreshes_duration_seconds",
			Help:      "Time spent refreshing blobs in seconds",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"storage_type", "operation"},
	)

	flatBlobAccessRefreshesSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "flat_blob_access_refreshes_size_bytes",
			Help:      "Size of blobs being refreshed in bytes",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"storage_type", "operation"},
	)
)

type flatBlobAccess struct {
	capabilities.Provider

	keyLocationMap  KeyLocationMap
	locationBlobMap LocationBlobMap
	digestKeyFormat digest.KeyFormat

	// lock protects OldCurrentNewLocationBlobMap state (block
	// rotation and allocation counters) and coordinates with
	// PeriodicSyncer's epoch boundaries. It is held exclusively for
	// the brief Put/Refresh allocations that may rotate blocks, and
	// in read mode for everything else — including the finalize
	// step. Per-blob KeyLocationMap and PersistentBlockList
	// invariants are now enforced inside those types themselves
	// (hashingKeyLocationMap.mu and PersistentBlockList.mu), so
	// multiple finalizes can run concurrently with each other and
	// with Get readers under this outer RLock.
	lock *sync.RWMutex
	// refreshStripes serialises refreshes per blob digest while
	// letting refreshes of different digests run in parallel. The
	// stripe index is the first byte of the SHA-256 key (uniform).
	// Replaces the prior single refreshLock, which dedup'd at the
	// cost of single-threading the entire refresh pipeline.
	refreshStripes [refreshStripeCount]sync.Mutex
	// refreshSemaphore bounds how many refreshes of distinct blobs
	// may be in flight at once, so that lifting the old
	// single-threaded refresh limit does not let a refresh storm
	// consume unbounded read and write bandwidth. A slot is always
	// taken before ba.lock, never while holding it.
	refreshSemaphore chan struct{}

	refreshesBlobsGet              prometheus.Observer
	refreshesBlobsGetFromComposite prometheus.Observer
	refreshesBlobsFindMissing      prometheus.Observer

	refreshesBlobsDurationGet              prometheus.Observer
	refreshesBlobsDurationGetFromComposite prometheus.Observer
	refreshesBlobsDurationFindMissing      prometheus.Observer
	refreshesBlobsSizeGet                  prometheus.Observer
	refreshesBlbosSizeGetFromComposite     prometheus.Observer
	refreshesBlobsSizeFindMissing          prometheus.Observer
}

// NewFlatBlobAccess creates a BlobAccess that forwards all calls to
// KeyLocationMap and LocationBlobMap backend. It's called 'flat',
// because it assumes all objects are stored in a flat namespace. It
// either ignores the REv2 instance name in digests entirely, or it
// strongly partitions objects by instance name. It does not introduce
// any hierarchy.
//
// refreshConcurrency bounds the number of refreshes of distinct blobs
// that may run concurrently. Values <= 0 select
// DefaultRefreshConcurrency; values above refreshStripeCount are
// clamped to it, as refreshes are striped that many ways by digest.
func NewFlatBlobAccess(keyLocationMap KeyLocationMap, locationBlobMap LocationBlobMap, digestKeyFormat digest.KeyFormat, lock *sync.RWMutex, refreshConcurrency int, storageType string, capabilitiesProvider capabilities.Provider) blobstore.BlobAccess {
	flatBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(flatBlobAccessRefreshesBlobs)
		prometheus.MustRegister(flatBlobAccessRefreshesDurationSeconds)
		prometheus.MustRegister(flatBlobAccessRefreshesSizeBytes)
	})

	if refreshConcurrency <= 0 {
		refreshConcurrency = DefaultRefreshConcurrency
	}
	if refreshConcurrency > refreshStripeCount {
		refreshConcurrency = refreshStripeCount
	}

	return &flatBlobAccess{
		Provider: capabilitiesProvider,

		keyLocationMap:   keyLocationMap,
		locationBlobMap:  locationBlobMap,
		digestKeyFormat:  digestKeyFormat,
		lock:             lock,
		refreshSemaphore: make(chan struct{}, refreshConcurrency),

		refreshesBlobsGet:              flatBlobAccessRefreshesBlobs.WithLabelValues(storageType, "Get"),
		refreshesBlobsGetFromComposite: flatBlobAccessRefreshesBlobs.WithLabelValues(storageType, "GetFromComposite"),
		refreshesBlobsFindMissing:      flatBlobAccessRefreshesBlobs.WithLabelValues(storageType, "FindMissing"),

		refreshesBlobsDurationGet:              flatBlobAccessRefreshesDurationSeconds.WithLabelValues(storageType, "Get"),
		refreshesBlobsDurationGetFromComposite: flatBlobAccessRefreshesDurationSeconds.WithLabelValues(storageType, "GetFromComposite"),
		refreshesBlobsDurationFindMissing:      flatBlobAccessRefreshesDurationSeconds.WithLabelValues(storageType, "FindMissing"),
		refreshesBlobsSizeGet:                  flatBlobAccessRefreshesSizeBytes.WithLabelValues(storageType, "Get"),
		refreshesBlbosSizeGetFromComposite:     flatBlobAccessRefreshesSizeBytes.WithLabelValues(storageType, "GetFromComposite"),
		refreshesBlobsSizeFindMissing:          flatBlobAccessRefreshesSizeBytes.WithLabelValues(storageType, "FindMissing"),
	}
}

func (ba *flatBlobAccess) getKey(digest digest.Digest) Key {
	return NewKeyFromString(digest.GetKey(ba.digestKeyFormat))
}

// acquireRefreshSlot reserves one of the refresh concurrency slots,
// blocking until one is free or ctx is done. The returned function
// releases the slot.
//
// This must never be called while holding ba.lock. Slot holders need
// ba.lock to make progress, so a caller that blocked here while holding
// it would deadlock the refresh path. The established order is
// stripe -> refreshSemaphore -> ba.lock -> klm.mu -> bl.mu.
func (ba *flatBlobAccess) acquireRefreshSlot(ctx context.Context) (func(), error) {
	select {
	case ba.refreshSemaphore <- struct{}{}:
		return func() { <-ba.refreshSemaphore }, nil
	case <-ctx.Done():
		return nil, util.StatusFromContext(ctx)
	}
}

// finalizePut commits a Put: it calls the BlockList's finalizer (which
// records the offset and bumps PersistentBlockList epoch metadata
// under PersistentBlockList.mu internally) and inserts the
// key-location-map entry (under hashingKeyLocationMap.mu internally).
// Callers hold ba.lock in read mode — the inner types provide their
// own exclusion, so multiple finalizePut calls can run concurrently.
func (ba *flatBlobAccess) finalizePut(putFinalizer LocationBlobPutFinalizer, key Key) (Location, error) {
	location, err := putFinalizer()
	if err != nil {
		return Location{}, err
	}
	return location, ba.keyLocationMap.Put(key, location)
}

func (ba *flatBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	key := ba.getKey(blobDigest)

	// Look up the blob in storage while holding a read lock.
	ba.lock.RLock()
	location, err := ba.keyLocationMap.Get(key)
	if err != nil {
		ba.lock.RUnlock()
		return buffer.NewBufferFromError(err)
	}
	getter, needsRefresh := ba.locationBlobMap.Get(location)
	if !needsRefresh {
		// The blob doesn't need to be refreshed, so we can
		// return its data directly.
		b := getter(blobDigest)
		ba.lock.RUnlock()
		return b
	}
	ba.lock.RUnlock()

	// Blob was found, but it needs to be refreshed to ensure it
	// doesn't disappear. Retry loading the blob a second time, this
	// time holding a write lock. This allows us to mutate the
	// key-location map or allocate new space to copy the blob on
	// the fly.
	//
	// TODO: Instead of copying data on the fly, should this be done
	// immediately, so that we can prevent potential duplication by
	// picking up the refresh lock?
	refreshStart := time.Now()

	ba.lock.Lock()
	location, err = ba.keyLocationMap.Get(key)
	if err != nil {
		ba.lock.Unlock()
		return buffer.NewBufferFromError(err)
	}
	getter, needsRefresh = ba.locationBlobMap.Get(location)
	b := getter(blobDigest)
	if !needsRefresh {
		// Some other thread managed to refresh the blob before
		// we got the write lock. No need to copy anymore.
		ba.lock.Unlock()
		return b
	}

	// Allocate space for the copy.
	putWriter, err := ba.locationBlobMap.Put(location.SizeBytes)
	ba.lock.Unlock()
	if err != nil {
		b.Discard()
		return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to refresh blob"))
	}

	// Copy the object while it's been returned. Block until copying
	// has finished to apply back-pressure.
	//
	// Unlike the FindMissing() and GetFromComposite() refresh paths,
	// this one deliberately takes neither a refresh stripe nor a
	// refreshSemaphore slot. It is inline with a client read, so
	// queueing it behind other refreshes would reintroduce exactly
	// the read-path stall this change exists to remove. This matches
	// the upstream behaviour, where the single refreshLock was not
	// held here either (see the TODO above).
	b1, b2 := b.CloneStream()
	return b1.WithTask(func() error {
		putFinalizer := putWriter(b2)
		ba.lock.RLock()
		_, err := ba.finalizePut(putFinalizer, key)
		if err == nil {
			ba.refreshesBlobsGet.Observe(1)
			ba.refreshesBlobsSizeGet.Observe(float64(location.SizeBytes))
			ba.refreshesBlobsDurationGet.Observe(time.Since(refreshStart).Seconds())
		}
		ba.lock.RUnlock()
		if err != nil {
			return util.StatusWrap(err, "Failed to refresh blob")
		}
		return nil
	})
}

func (ba *flatBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	parentKey := ba.getKey(parentDigest)
	childKey := ba.getKey(childDigest)

	// Look up the blob in storage while holding a read lock. Even
	// though the child object determines the data to be returned,
	// the parent object controls whether it needs to be refreshed.
	// We therefore look up both unconditionally.
	ba.lock.RLock()
	parentLocation, err := ba.keyLocationMap.Get(parentKey)
	if err != nil {
		ba.lock.RUnlock()
		return buffer.NewBufferFromError(err)
	}
	if _, needsRefresh := ba.locationBlobMap.Get(parentLocation); !needsRefresh {
		if childLocation, err := ba.keyLocationMap.Get(childKey); err == nil {
			// The parent object doesn't need to be
			// refreshed, and the child object exists.
			// Return the child object immediately.
			childGetter, _ := ba.locationBlobMap.Get(childLocation)
			b := childGetter(childDigest)
			ba.lock.RUnlock()
			return b
		} else if status.Code(err) != codes.NotFound {
			ba.lock.RUnlock()
			return buffer.NewBufferFromError(err)
		}
	}
	ba.lock.RUnlock()

	// The parent object was found, but it either hasn't been sliced
	// yet, or it needs to be refreshed to ensure it doesn't
	// disappear. Retry the process above, but now with write locks
	// acquired. The per-digest stripe lock serialises refreshes of
	// the same parent across concurrent GetFromComposite calls; it
	// does not block refreshes of other parents.
	parentRefreshStripe := &ba.refreshStripes[parentKey[0]]
	parentRefreshStripe.Lock()
	defer parentRefreshStripe.Unlock()

	releaseRefreshSlot, err := ba.acquireRefreshSlot(ctx)
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	defer releaseRefreshSlot()

	ba.lock.Lock()
	parentLocation, err = ba.keyLocationMap.Get(parentKey)
	if err != nil {
		ba.lock.Unlock()
		return buffer.NewBufferFromError(err)
	}

	var bParentSlicing buffer.Buffer
	var putFinalizer LocationBlobPutFinalizer
	parentGetter, needsRefresh := ba.locationBlobMap.Get(parentLocation)
	// Add refresh start time
	refreshStart := time.Now()
	if needsRefresh {
		// The parent object needs to be refreshed and sliced.
		bParent := parentGetter(parentDigest)
		putWriter, err := ba.locationBlobMap.Put(parentLocation.SizeBytes)
		ba.lock.Unlock()
		if err != nil {
			bParent.Discard()
			return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to refresh blob"))
		}

		// Copy the data while it's being sliced.
		bParent1, bParent2 := bParent.CloneStream()
		bParentSlicing = bParent1.WithTask(func() error {
			putFinalizer = putWriter(bParent2)
			return nil
		})
	} else {
		if childLocation, err := ba.keyLocationMap.Get(childKey); err == nil {
			// The parent object was refreshed and sliced in
			// the meantime.
			childGetter, _ := ba.locationBlobMap.Get(childLocation)
			b := childGetter(childDigest)
			ba.lock.Unlock()
			return b
		} else if status.Code(err) != codes.NotFound {
			ba.lock.Unlock()
			return buffer.NewBufferFromError(err)
		}

		// The parent object only needs to be sliced.
		bParentSlicing = parentGetter(parentDigest)
		ba.lock.Unlock()
	}

	// Perform the slicing.
	bChild, slices := slicer.Slice(bParentSlicing, childDigest)
	sliceKeys := make([]Key, 0, len(slices))
	for _, slice := range slices {
		sliceKeys = append(sliceKeys, ba.getKey(slice.Digest))
	}

	// Complete refreshing in case it was performed, and insert the
	// per-slice key-location entries. We hold ba.lock in read mode
	// because hashingKeyLocationMap and PersistentBlockList enforce
	// their own internal exclusion; we only need ba.lock here to
	// keep block-state from being rotated out by a concurrent Put
	// allocation (which takes ba.lock for writing).
	ba.lock.RLock()
	if needsRefresh {
		parentLocation, err = ba.finalizePut(putFinalizer, parentKey)
		// Add size metric before refresh
		ba.refreshesBlbosSizeGetFromComposite.Observe(float64(parentLocation.SizeBytes))
		if err != nil {
			ba.lock.RUnlock()
			bChild.Discard()
			return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to refresh blob"))
		}
		ba.refreshesBlobsDurationGetFromComposite.Observe(time.Since(refreshStart).Seconds())
		ba.refreshesBlobsGetFromComposite.Observe(1)
	}

	// Create key-location map entries for each of the slices. This
	// permits subsequent GetFromComposite() calls to access the
	// individual parts without any slicing.
	for i, slice := range slices {
		if err := ba.keyLocationMap.Put(sliceKeys[i], Location{
			BlockIndex:  parentLocation.BlockIndex,
			OffsetBytes: parentLocation.OffsetBytes + slice.OffsetBytes,
			SizeBytes:   slice.SizeBytes,
		}); err != nil {
			ba.lock.RUnlock()
			bChild.Discard()
			return buffer.NewBufferFromError(util.StatusWrapf(err, "Failed to create child blob %#v", slice.Digest.String()))
		}
	}
	ba.lock.RUnlock()
	return bChild
}

func (ba *flatBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}

	// Allocate space to store the object.
	ba.lock.Lock()
	putWriter, err := ba.locationBlobMap.Put(sizeBytes)
	ba.lock.Unlock()
	if err != nil {
		b.Discard()
		return err
	}

	// Ingest the data associated with the object. This must be done
	// without holding any locks, so that I/O can happen in
	// parallel.
	putFinalizer := putWriter(b)

	key := ba.getKey(blobDigest)
	ba.lock.RLock()
	_, err = ba.finalizePut(putFinalizer, key)
	ba.lock.RUnlock()
	return err
}

func (ba *flatBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Convert all digests to Keys.
	keys := make([]Key, 0, digests.Length())
	for _, blobDigest := range digests.Items() {
		keys = append(keys, ba.getKey(blobDigest))
	}

	// Perform an initial scan to determine which blobs are present
	// in storage.
	type blobToRefresh struct {
		digest digest.Digest
		key    Key
	}
	var blobsToRefresh []blobToRefresh
	missing := digest.NewSetBuilder(0)
	ba.lock.RLock()
	for i, blobDigest := range digests.Items() {
		key := keys[i]
		if location, err := ba.keyLocationMap.Get(key); err == nil {
			_, needsRefresh := ba.locationBlobMap.Get(location)
			if needsRefresh {
				// Blob is present, but it must be
				// refreshed for it to remain present.
				// Enqueue it for the second scan.
				blobsToRefresh = append(blobsToRefresh, blobToRefresh{
					digest: blobDigest,
					key:    key,
				})
			}
		} else if status.Code(err) == codes.NotFound {
			// Blob is absent.
			missing.Add(blobDigest)
		} else {
			ba.lock.RUnlock()
			return digest.EmptySet, util.StatusWrapf(err, "Failed to get blob %#v", blobDigest.String())
		}
	}
	ba.lock.RUnlock()
	if len(blobsToRefresh) == 0 {
		return missing.Build(), nil
	}

	// One or more blobs need to be refreshed.
	//
	// Per-digest stripe locks serialise concurrent refreshes of the
	// SAME blob (preserving the dedup property that the prior single
	// refreshLock provided) while letting refreshes of DIFFERENT
	// blobs run in parallel. Stripe index = key[0] (first byte of
	// the SHA-256 Key).
	//
	// The old code also used that single lock to limit refresh
	// bandwidth to one thread. That limit is preserved, but as an
	// explicit and configurable one: refreshSemaphore caps how many
	// refreshes of distinct blobs may be in flight, defaulting to
	// DefaultRefreshConcurrency rather than to 1.
	refreshStart := time.Now()
	blobsRefreshedSuccessfully := 0
	var blobRefreshSizeBytes int64
	// Re-acquire ba.lock per blob rather than holding it for the
	// entire loop. The old behaviour pinned the outer lock across
	// every iteration's klm.Get + locationBlobMap.Get + alloc,
	// which during an old-to-new migration starved Get/Put on the
	// outer lock. Per-iteration locking lets other operations
	// interleave between refreshes.
	for _, blobToRefresh := range blobsToRefresh {
		// refreshOne returns the per-blob outcome; using a closure
		// gives us defer-based stripe unlock without leaking any
		// exit path.
		refreshed, sizeBytes, isMissing, err := func() (refreshed bool, sizeBytes int64, isMissing bool, err error) {
			stripe := &ba.refreshStripes[blobToRefresh.key[0]]
			stripe.Lock()
			defer stripe.Unlock()

			releaseSlot, err := ba.acquireRefreshSlot(ctx)
			if err != nil {
				return false, 0, false, err
			}
			defer releaseSlot()

			ba.lock.Lock()
			location, lookupErr := ba.keyLocationMap.Get(blobToRefresh.key)
			if lookupErr != nil {
				ba.lock.Unlock()
				if status.Code(lookupErr) == codes.NotFound {
					// Blob disappeared between the first and
					// second scan. Simply report it as missing.
					return false, 0, true, nil
				}
				return false, 0, false, util.StatusWrapf(lookupErr, "Failed to get blob %#v", blobToRefresh.digest.String())
			}
			getter, needsRefresh := ba.locationBlobMap.Get(location)
			if !needsRefresh {
				// Another concurrent refresh (same stripe) won the
				// race and migrated the blob while we waited.
				ba.lock.Unlock()
				return false, 0, false, nil
			}
			b := getter(blobToRefresh.digest)
			putWriter, allocErr := ba.locationBlobMap.Put(location.SizeBytes)
			ba.lock.Unlock()
			if allocErr != nil {
				b.Discard()
				return false, 0, false, util.StatusWrapf(allocErr, "Failed to refresh blob %#v", blobToRefresh.digest.String())
			}

			// Copy the data while unlocked, so that concurrent
			// requests for other data continue to be serviced.
			putFinalizer := putWriter(b)

			ba.lock.RLock()
			_, finErr := ba.finalizePut(putFinalizer, blobToRefresh.key)
			ba.lock.RUnlock()
			if finErr != nil {
				return false, 0, false, util.StatusWrapf(finErr, "Failed to refresh blob %#v", blobToRefresh.digest.String())
			}
			return true, location.SizeBytes, false, nil
		}()
		if err != nil {
			return digest.EmptySet, err
		}
		if isMissing {
			missing.Add(blobToRefresh.digest)
			continue
		}
		if refreshed {
			blobsRefreshedSuccessfully++
			blobRefreshSizeBytes += sizeBytes
		}
	}
	ba.refreshesBlobsFindMissing.Observe(float64(blobsRefreshedSuccessfully))
	ba.refreshesBlobsDurationFindMissing.Observe(time.Since(refreshStart).Seconds())
	ba.refreshesBlobsSizeFindMissing.Observe(float64(blobRefreshSizeBytes))
	return missing.Build(), nil
}
