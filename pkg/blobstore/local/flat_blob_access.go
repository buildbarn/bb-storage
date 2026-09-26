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

	refreshesBlobsGet         prometheus.Observer
	refreshesBlobsFindMissing prometheus.Observer

	refreshesBlobsDurationGet         prometheus.Observer
	refreshesBlobsDurationFindMissing prometheus.Observer
	refreshesBlobsSizeGet             prometheus.Observer
	refreshesBlobsSizeFindMissing     prometheus.Observer
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

		if currentLocation != location {
			// We came back from acquiring the lock and now the map
			// points to a new position in the block storage, the value
			// was either overwritten or refreshed by another thread. We
			// try again.
			ba.lock.Unlock()
			continue
		}

		// Allocate space for the copy.
		putWriter, err := ba.locationBlobMap.Put(currentLocation.SizeBytes)
		ba.lock.Unlock()
		if err != nil {
			return zero, util.StatusWrap(err, "Failed to refresh blob")
		}

		// Copy the object.
		putFinalizer := putWriter(data)

		ba.lock.Lock()
		_, err = ba.finalizePut(putFinalizer, key)
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
	ba.lock.Lock()
	putWriter, err := ba.locationBlobMap.Put(int64(len(data)))
	ba.lock.Unlock()
	if err != nil {
		return err
	}

	// Ingest the data associated with the object. This must be done
	// without holding any locks, so that I/O can happen in
	// parallel.
	putFinalizer := putWriter(data)

	key := ba.getKey(blobDigest)
	ba.lock.Lock()
	_, err = ba.finalizePut(putFinalizer, key)
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

		if currentLocation != location {
			// When acquiring the write lock and the map now points to a
			// new position in the block storage, the value was either
			// overwritten or refreshed by another thread. We don't have
			// to do anything for this blob.
			ba.lock.Unlock()
			continue
		}

		blobRefreshSizeBytes += currentLocation.SizeBytes
		putWriter, err := ba.locationBlobMap.Put(currentLocation.SizeBytes)

		// Copy the data outside of a lock.
		ba.lock.Unlock()
		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}
		putFinalizer := putWriter(data)

		ba.lock.Lock()
		if _, err := ba.finalizePut(putFinalizer, blobToRefresh.key); err != nil {
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
