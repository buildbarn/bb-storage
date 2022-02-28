package local

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	flatBlobAccessPrometheusMetrics sync.Once

	flatBlobAccessRefreshes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "flat_blob_access_refreshes",
			Help:      "The number of blobs that were refreshed when requested",
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1.0, 2.0, 16)...),
		},
		[]string{"storage_type", "operation"})
)

type flatBlobAccess struct {
	capabilities.Provider

	keyBlobMap      KeyBlobMap
	digestKeyFormat digest.KeyFormat

	lock        *sync.RWMutex
	refreshLock sync.Mutex

	refreshesGet         prometheus.Observer
	refreshesFindMissing prometheus.Observer
}

// NewFlatBlobAccess creates a BlobAccess that forwards all calls to a
// KeyBlobMap backend. It's called 'flat', because it assumes all
// objects are stored in a flat namespace. It either ignores the REv2
// instance name in digests entirely, or it strongly partitions objects
// by instance name. It does not introduce any hierarchy.
func NewFlatBlobAccess(keyBlobMap KeyBlobMap, digestKeyFormat digest.KeyFormat, lock *sync.RWMutex, storageType string, capabilitiesProvider capabilities.Provider) blobstore.BlobAccess {
	flatBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(flatBlobAccessRefreshes)
	})

	return &flatBlobAccess{
		Provider: capabilitiesProvider,

		keyBlobMap:      keyBlobMap,
		digestKeyFormat: digestKeyFormat,
		lock:            lock,

		refreshesGet:         flatBlobAccessRefreshes.WithLabelValues(storageType, "Get"),
		refreshesFindMissing: flatBlobAccessRefreshes.WithLabelValues(storageType, "FindMissing"),
	}
}

func (ba *flatBlobAccess) getKey(digest digest.Digest) Key {
	return NewKeyFromString(digest.GetKey(ba.digestKeyFormat))
}

func (ba *flatBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	key := ba.getKey(blobDigest)

	// Look up the blob in storage while holding a read lock.
	ba.lock.RLock()
	getter, _, needsRefresh, err := ba.keyBlobMap.Get(key)
	if err != nil {
		ba.lock.RUnlock()
		return buffer.NewBufferFromError(err)
	}
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
	// time holding a write lock. This allows us to allocate new
	// space to copy the blob on the fly.
	//
	// TODO: Instead of copying data on the fly, should this be done
	// immediately, so that we can prevent potential duplication by
	// picking up the refresh lock?
	ba.lock.Lock()
	getter, sizeBytes, needsRefresh, err := ba.keyBlobMap.Get(key)
	if err != nil {
		ba.lock.Unlock()
		return buffer.NewBufferFromError(err)
	}
	b := getter(blobDigest)
	if !needsRefresh {
		// Some other thread managed to refresh the blob before
		// we got the write lock. No need to copy anymore.
		ba.lock.Unlock()
		return b
	}

	// Allocate space for the copy.
	putWriter, err := ba.keyBlobMap.Put(sizeBytes)
	ba.lock.Unlock()
	if err != nil {
		b.Discard()
		return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to refresh blob"))
	}

	// Copy the object while it's been returned. Block until copying
	// has finished to apply back-pressure.
	b1, b2 := b.CloneStream()
	return b1.WithTask(func() error {
		putFinalizer := putWriter(b2)
		ba.lock.Lock()
		err := putFinalizer(key)
		if err == nil {
			ba.refreshesGet.Observe(1)
		}
		ba.lock.Unlock()
		if err != nil {
			return util.StatusWrap(err, "Failed to refresh blob")
		}
		return nil
	})
}

func (ba *flatBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}

	// Allocate space to store the object.
	ba.lock.Lock()
	putWriter, err := ba.keyBlobMap.Put(sizeBytes)
	ba.lock.Unlock()
	if err != nil {
		b.Discard()
		return err
	}

	// Copy the the object into storage. This can be done without
	// holding any locks, so that I/O can happen in parallel.
	putFinalizer := putWriter(b)

	key := ba.getKey(blobDigest)
	ba.lock.Lock()
	err = putFinalizer(key)
	ba.lock.Unlock()
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
	missing := digest.NewSetBuilder()
	ba.lock.RLock()
	for i, blobDigest := range digests.Items() {
		key := keys[i]
		if _, _, needsRefresh, err := ba.keyBlobMap.Get(key); err == nil {
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

	blobsRefreshedSuccessfully := 0
	ba.lock.Lock()
	for _, blobToRefresh := range blobsToRefresh {
		if getter, sizeBytes, needsRefresh, err := ba.keyBlobMap.Get(blobToRefresh.key); err == nil {
			if needsRefresh {
				// Blob is present and still needs to be
				// refreshed. Allocate space for a copy.
				b := getter(blobToRefresh.digest)
				putWriter, err := ba.keyBlobMap.Put(sizeBytes)
				ba.lock.Unlock()
				if err != nil {
					b.Discard()
					return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
				}

				// Copy the data while unlocked, so that
				// concurrent requests for other data
				// continue to be serviced.
				putFinalizer := putWriter(b)

				ba.lock.Lock()
				if err := putFinalizer(blobToRefresh.key); err != nil {
					ba.lock.Unlock()
					return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
				}
				blobsRefreshedSuccessfully++
			}
		} else if status.Code(err) == codes.NotFound {
			// Blob disappeared between the first and second
			// scan. Simply report it as missing.
			missing.Add(blobToRefresh.digest)
		} else {
			ba.lock.Unlock()
			return digest.EmptySet, util.StatusWrapf(err, "Failed to get blob %#v", blobToRefresh.digest.String())
		}
	}
	ba.lock.Unlock()
	ba.refreshesFindMissing.Observe(float64(blobsRefreshedSuccessfully))
	return missing.Build(), nil
}
