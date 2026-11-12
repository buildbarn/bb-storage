package local

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

type flatBlobAccess[T any] struct {
	capabilities.Provider

	coder                  coder.Coder[T, []byte]
	keyLocationMap         KeyLocationMap
	blockReferenceResolver BlockReferenceResolver
	locationBlobMap        LocationBlobMap
	digestKeyFormat        digest.KeyFormat

	lock        *sync.RWMutex
	refreshLock sync.Mutex
	// Writes that have allocated space but have not been finalized,
	// by key. Protected by lock.
	pendingWrites map[Key]*pendingWrites

	refreshesBlobsGet         prometheus.Observer
	refreshesBlobsFindMissing prometheus.Observer

	refreshesBlobsDurationGet         prometheus.Observer
	refreshesBlobsDurationFindMissing prometheus.Observer
	refreshesBlobsSizeGet             prometheus.Observer
	refreshesBlobsSizeFindMissing     prometheus.Observer
}

// pendingWrites tracks the writes of a single key that have allocated
// space but have not been finalized. It is used to prevent a refresh
// from replacing the value of a Put() that overlaps with it.
type pendingWrites struct {
	writes       int
	putsInFlight int
	putsStarted  uint64
}

// NewFlatBlobAccess creates a BlobAccess that forwards all calls to
// KeyLocationMap and LocationBlobMap backend. It's called 'flat',
// because it assumes all objects are stored in a flat namespace. It
// either ignores the REv2 instance name in digests entirely, or it
// strongly partitions objects by instance name. It does not introduce
// any hierarchy.
func NewFlatBlobAccess[T any](keyLocationMap KeyLocationMap, blockReferenceResolver BlockReferenceResolver, locationBlobMap LocationBlobMap, digestKeyFormat digest.KeyFormat, lock *sync.RWMutex, storageType string, capabilitiesProvider capabilities.Provider, coder coder.Coder[T, []byte]) blobstore.BlobAccess[T] {
	flatBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(flatBlobAccessRefreshesBlobs)
		prometheus.MustRegister(flatBlobAccessRefreshesDurationSeconds)
		prometheus.MustRegister(flatBlobAccessRefreshesSizeBytes)
	})

	return &flatBlobAccess[T]{
		Provider: capabilitiesProvider,

		coder:                  coder,
		keyLocationMap:         keyLocationMap,
		blockReferenceResolver: blockReferenceResolver,
		locationBlobMap:        locationBlobMap,
		digestKeyFormat:        digestKeyFormat,
		lock:                   lock,
		pendingWrites:          map[Key]*pendingWrites{},

		refreshesBlobsGet:         flatBlobAccessRefreshesBlobs.WithLabelValues(storageType, "Get"),
		refreshesBlobsFindMissing: flatBlobAccessRefreshesBlobs.WithLabelValues(storageType, "FindMissing"),

		refreshesBlobsDurationGet:         flatBlobAccessRefreshesDurationSeconds.WithLabelValues(storageType, "Get"),
		refreshesBlobsDurationFindMissing: flatBlobAccessRefreshesDurationSeconds.WithLabelValues(storageType, "FindMissing"),
		refreshesBlobsSizeGet:             flatBlobAccessRefreshesSizeBytes.WithLabelValues(storageType, "Get"),
		refreshesBlobsSizeFindMissing:     flatBlobAccessRefreshesSizeBytes.WithLabelValues(storageType, "FindMissing"),
	}
}

func (ba *flatBlobAccess[T]) getKey(digest digest.Digest) Key {
	return NewKeyFromString(digest.GetKey(ba.digestKeyFormat))
}

// finalizePut is called to finalize a write to the data store. This
// method must be called while holding the write lock.
func (ba *flatBlobAccess[T]) finalizePut(putFinalizer LocationBlobPutFinalizer, key Key) (Location, error) {
	location, err := putFinalizer()
	if err != nil {
		return Location{}, err
	}
	return location, ba.keyLocationMap.Put(key, location, ba.blockReferenceResolver)
}

// locationUnchanged returns whether currentLocation is the location
// that was observed before the lock was last dropped. Block indices
// shift as blocks are released, so the block is identified through the
// BlockReference obtained at that time. This method must be called
// while holding the lock.
func (ba *flatBlobAccess[T]) locationUnchanged(blockReference BlockReference, location, currentLocation Location) bool {
	blockIndex, _, found := ba.blockReferenceResolver.BlockReferenceToBlockIndex(blockReference)
	return found &&
		blockIndex == currentLocation.BlockIndex &&
		location.OffsetBytes == currentLocation.OffsetBytes &&
		location.SizeBytes == currentLocation.SizeBytes
}

func (ba *flatBlobAccess[T]) beginWrite(key Key) *pendingWrites {
	pw, ok := ba.pendingWrites[key]
	if !ok {
		pw = &pendingWrites{}
		ba.pendingWrites[key] = pw
	}
	pw.writes++
	return pw
}

func (ba *flatBlobAccess[T]) endWrite(key Key, pw *pendingWrites) {
	pw.writes--
	if pw.writes == 0 {
		delete(ba.pendingWrites, key)
	}
}

// beginPut registers a Put() of a key after it has allocated space.
// This method must be called while holding the write lock.
func (ba *flatBlobAccess[T]) beginPut(key Key) *pendingWrites {
	pw := ba.beginWrite(key)
	pw.putsInFlight++
	pw.putsStarted++
	return pw
}

// finalizeRefresh is like finalizePut(), except that the key-location
// map is left untouched if a Put() of the same key started after the
// refresh. Such a Put() normally obtains a newer location, but the
// key-location map may have dropped its record while its block is
// still present, after which publishing the copy would bring back the
// old value. This method must be called while holding the write lock.
func (ba *flatBlobAccess[T]) finalizeRefresh(putFinalizer LocationBlobPutFinalizer, key Key, pw *pendingWrites, putsStarted uint64) error {
	location, err := putFinalizer()
	overtaken := pw.putsStarted != putsStarted
	ba.endWrite(key, pw)
	if err != nil || overtaken {
		return err
	}
	return ba.keyLocationMap.Put(key, location, ba.blockReferenceResolver)
}

func (ba *flatBlobAccess[T]) Get(ctx context.Context, blobDigest digest.Digest) (T, error) {
	key := ba.getKey(blobDigest)
	var zero T

	for {
		// Look up the blob in storage while holding a read lock.
		ba.lock.RLock()
		location, err := ba.keyLocationMap.Get(key, ba.blockReferenceResolver)
		if err != nil {
			ba.lock.RUnlock()
			return zero, err
		}
		getter, needsRefresh := ba.locationBlobMap.Get(location)
		data, integrityCallback, err := getter(blobDigest)
		var blockReference BlockReference
		if needsRefresh {
			blockReference, _ = ba.blockReferenceResolver.BlockIndexToBlockReference(location.BlockIndex)
		}
		ba.lock.RUnlock()

		if err != nil {
			return zero, err
		}

		val, err := ba.coder.Decode(data, blobDigest)
		if err != nil {
			integrityCallback()
			return zero, util.StatusWrapWithCode(err, codes.NotFound, "Blob did not decode when read")
		}

		if !needsRefresh {
			// The blob doesn't need to be refreshed, so we can
			// return its data directly.
			return val, nil
		}

		// Blob was found, but it needs to be refreshed to ensure it
		// doesn't disappear. Retry loading the blob a second time, this
		// time holding a write lock. This allows us to mutate the
		// key-location map or allocate new space to copy the blob on
		// the fly.
		refreshStart := time.Now()

		ba.lock.Lock()
		currentLocation, err := ba.keyLocationMap.Get(key, ba.blockReferenceResolver)
		if err != nil {
			ba.lock.Unlock()
			return zero, err
		}

		if !ba.locationUnchanged(blockReference, location, currentLocation) {
			// We came back from acquiring the lock and now the map
			// points to a new position in the block storage, the value
			// was either overwritten or refreshed by another thread. We
			// try again.
			ba.lock.Unlock()
			continue
		}

		// A Put() of this key is in flight. It obtained an older
		// location than the copy would, so publishing the copy
		// would hide its value, even if it finalizes first. Don't
		// refresh. If the Put() fails, a later call may still
		// refresh the entry.
		if pw, ok := ba.pendingWrites[key]; ok && pw.putsInFlight > 0 {
			ba.lock.Unlock()
			return val, nil
		}

		// Allocate space for the copy.
		putWriter, err := ba.locationBlobMap.Put(currentLocation.SizeBytes)
		if err != nil {
			ba.lock.Unlock()
			return zero, util.StatusWrap(err, "Failed to refresh blob")
		}
		pw := ba.beginWrite(key)
		putsStarted := pw.putsStarted
		ba.lock.Unlock()

		// Copy the object.
		putFinalizer := putWriter(data)

		ba.lock.Lock()
		err = ba.finalizeRefresh(putFinalizer, key, pw, putsStarted)
		if err == nil {
			ba.refreshesBlobsGet.Observe(1)
			ba.refreshesBlobsSizeGet.Observe(float64(currentLocation.SizeBytes))
			ba.refreshesBlobsDurationGet.Observe(time.Since(refreshStart).Seconds())
		}
		ba.lock.Unlock()
		if err != nil {
			return zero, util.StatusWrap(err, "Failed to refresh blob")
		}
		return val, nil
	}
}

func (ba *flatBlobAccess[T]) Put(ctx context.Context, blobDigest digest.Digest, value T) error {
	// Get the byte representation of the supplied object.
	data, err := ba.coder.Encode(value, blobDigest)
	if err != nil {
		return err
	}

	// Allocate space to store the object.
	key := ba.getKey(blobDigest)
	ba.lock.Lock()
	putWriter, err := ba.locationBlobMap.Put(int64(len(data)))
	if err != nil {
		ba.lock.Unlock()
		return err
	}
	pw := ba.beginPut(key)
	ba.lock.Unlock()

	// Ingest the data associated with the object. This must be done
	// without holding any locks, so that I/O can happen in
	// parallel.
	putFinalizer := putWriter(data)

	ba.lock.Lock()
	_, err = ba.finalizePut(putFinalizer, key)
	pw.putsInFlight--
	ba.endWrite(key, pw)
	ba.lock.Unlock()
	return err
}

func (ba *flatBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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
		if location, err := ba.keyLocationMap.Get(key, ba.blockReferenceResolver); err == nil {
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
	// We should prevent concurrent FindMissing() calls from
	// refreshing the same blobs, as that would cause data to be
	// duplicated and load to increase significantly. Pick up the
	// refresh lock to ensure bandwidth of refreshing is limited to
	// one thread.
	ba.refreshLock.Lock()
	defer ba.refreshLock.Unlock()
	// Add refresh start time before the refresh loop
	refreshStart := time.Now()
	blobsRefreshedSuccessfully := 0
	var blobRefreshSizeBytes int64
	for _, blobToRefresh := range blobsToRefresh {
		// Read blob data from the backing store.
		ba.lock.RLock()
		location, err := ba.keyLocationMap.Get(blobToRefresh.key, ba.blockReferenceResolver)
		if err != nil {
			ba.lock.RUnlock()
			if status.Code(err) == codes.NotFound {
				// Blob disappeared between the first and second scan.
				// Simply report it as missing.
				missing.Add(blobToRefresh.digest)
				continue
			}
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}

		getter, needsRefresh := ba.locationBlobMap.Get(location)
		if !needsRefresh {
			// Blob was refreshed by someone else.
			ba.lock.RUnlock()
			continue
		}
		data, integrityCallback, err := getter(blobToRefresh.digest)
		blockReference, _ := ba.blockReferenceResolver.BlockIndexToBlockReference(location.BlockIndex)
		ba.lock.RUnlock()

		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}
		if _, err := ba.coder.Decode(data, blobToRefresh.digest); err != nil {
			integrityCallback()
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}

		// Acquire write lock to allocate area for data.
		ba.lock.Lock()
		currentLocation, err := ba.keyLocationMap.Get(blobToRefresh.key, ba.blockReferenceResolver)
		if err != nil {
			ba.lock.Unlock()
			if status.Code(err) == codes.NotFound {
				// Blob disappeared before acquiring write lock. Simply
				// report it as missing.
				missing.Add(blobToRefresh.digest)
				continue
			}
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}

		if !ba.locationUnchanged(blockReference, location, currentLocation) {
			// When acquiring the write lock and the map now points to a
			// new position in the block storage, the value was either
			// overwritten or refreshed by another thread. We don't have
			// to do anything for this blob.
			ba.lock.Unlock()
			continue
		}

		if pw, ok := ba.pendingWrites[blobToRefresh.key]; ok && pw.putsInFlight > 0 {
			// A Put() of this blob is in flight, and publishing
			// a copy would hide its value. See Get().
			ba.lock.Unlock()
			continue
		}

		blobRefreshSizeBytes += currentLocation.SizeBytes
		putWriter, err := ba.locationBlobMap.Put(currentLocation.SizeBytes)
		if err != nil {
			ba.lock.Unlock()
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}
		pw := ba.beginWrite(blobToRefresh.key)
		putsStarted := pw.putsStarted

		// Copy the data outside of a lock.
		ba.lock.Unlock()
		putFinalizer := putWriter(data)

		ba.lock.Lock()
		if err := ba.finalizeRefresh(putFinalizer, blobToRefresh.key, pw, putsStarted); err != nil {
			ba.lock.Unlock()
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}
		blobsRefreshedSuccessfully++
		ba.lock.Unlock()
	}
	ba.refreshesBlobsFindMissing.Observe(float64(blobsRefreshedSuccessfully))
	ba.refreshesBlobsDurationFindMissing.Observe(time.Since(refreshStart).Seconds())
	ba.refreshesBlobsSizeFindMissing.Observe(float64(blobRefreshSizeBytes))
	return missing.Build(), nil
}
