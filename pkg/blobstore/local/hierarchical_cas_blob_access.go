package local

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hierarchicalCSBlobAccess struct {
	capabilities.Provider

	coder                  coder.Coder[*chunk.Chunk, []byte]
	keyLocationMap         KeyLocationMap
	blockReferenceResolver BlockReferenceResolver
	locationBlobMap        LocationBlobMap

	lock        *sync.RWMutex
	refreshLock sync.Mutex
}

// NewHierarchicalCSBlobAccess creates a BlobAccess that uses a
// KeyLocationMap and a LocationBlobMap as backing stores.
//
// The BlobAccess returned by this function can be thought of as being
// an alternative to FlatBlobAccess, with one big difference: it keeps
// track of which REv2 instance name prefixes are permitted to access an
// object. It does this by writing multiple entries into the
// key-location map:
//
//   - One canonical entry that always points to the newest copy of an
//     object. This entry's key does not contain an instance name.
//   - One or more lookup entries, whose keys contain an instance name
//     prefix. These are only synchronized with the canonical entry when
//     the lookup entry points to an object that needs to be refreshed.
//
// As the name implies, this implementation should only be used for the
// Chunk Storage (CS). This is because writes for objects that already
// exist for a different REv2 instance name don't cause any new data to
// be ingested. This makes this implementation unsuitable for mutable
// data sets.
func NewHierarchicalCSBlobAccess(keyLocationMap KeyLocationMap, blockReferenceResolver BlockReferenceResolver, locationBlobMap LocationBlobMap, lock *sync.RWMutex, capabilitiesProvider capabilities.Provider, coder coder.Coder[*chunk.Chunk, []byte]) blobstore.BlobAccess[*chunk.Chunk] {
	return &hierarchicalCSBlobAccess{
		Provider: capabilitiesProvider,

		coder:                  coder,
		keyLocationMap:         keyLocationMap,
		blockReferenceResolver: blockReferenceResolver,
		locationBlobMap:        locationBlobMap,
		lock:                   lock,
	}
}

// getMostSpecificLookupKey returns the Key that should be used for
// object lookups that contains the entire REv2 instance name. This is
// the Key that is used during Put() operations.
func getMostSpecificLookupKey(blobDigest digest.Digest) Key {
	return NewKeyFromString(blobDigest.GetKey(digest.KeyWithInstance))
}

// getAllLookupKeys returns a list of all Keys that should be queried
// when doing lookups. These are used as part of Get() and
// FindMissing().
func getAllLookupKeys(blobDigest digest.Digest) []Key {
	parentDigests := blobDigest.GetDigestsWithParentInstanceNames()
	keys := make([]Key, 0, len(parentDigests))
	for _, parentDigest := range parentDigests {
		keys = append(keys, getMostSpecificLookupKey(parentDigest))
	}
	return keys
}

// getCanonicalKey returns the Key that uniquely identifies the object's
// contents. It is used to prevent storing the same object redundantly.
func getCanonicalKey(blobDigest digest.Digest) Key {
	return NewKeyFromString(blobDigest.GetKey(digest.KeyWithoutInstance))
}

var errKeyLocationMapNotFound = status.Error(codes.NotFound, "Object not found")

// getLeastSpecificLookupEntry searches the key-location for an object,
// given a list of lookup Keys. It returns the first Key (with the
// shortest instance name) for which a match occurred, together with a
// Location at which the object is stored.
func (ba *hierarchicalCSBlobAccess) getLeastSpecificLookupEntry(lookupKeys []Key) (Key, Location, error) {
	for _, lookupKey := range lookupKeys {
		if location, err := ba.keyLocationMap.Get(lookupKey, ba.blockReferenceResolver); err == nil {
			return lookupKey, location, nil
		} else if status.Code(err) != codes.NotFound {
			return Key{}, Location{}, err
		}
	}
	return Key{}, Location{}, errKeyLocationMapNotFound
}

// syncFromCanonicalEntry attempts to synchronize a lookup entry in the
// key-location map to point to the canonical version of an object, if
// it exists and doesn't need to be refreshed.
//
// This method can be used to refresh a key-location map without
// necessarily copying the data of the underlying object.
func (ba *hierarchicalCSBlobAccess) syncFromCanonicalEntry(canonicalKey, lookupKey Key) error {
	canonicalLocation, err := ba.keyLocationMap.Get(canonicalKey, ba.blockReferenceResolver)
	if err != nil {
		return err
	}
	_, needsRefresh := ba.locationBlobMap.Get(canonicalLocation)
	if needsRefresh {
		return status.Error(codes.NotFound, "Canonical entry needs to be refreshed")
	}
	return ba.keyLocationMap.Put(lookupKey, canonicalLocation, ba.blockReferenceResolver)
}

// finalizePut is called to finalize a write to the data store. This
// method must be called while holding the write lock.
func (ba *hierarchicalCSBlobAccess) finalizePut(putFinalizer LocationBlobPutFinalizer, canonicalKey, lookupKey Key) error {
	// Finalize the write of the data.
	location, err := putFinalizer()
	if err != nil {
		return err
	}

	// Store two key-location map entries: one for the canonical key
	// and one for the lookup key.
	if err := ba.keyLocationMap.Put(canonicalKey, location, ba.blockReferenceResolver); err != nil {
		return err
	}
	return ba.keyLocationMap.Put(lookupKey, location, ba.blockReferenceResolver)
}

func (ba *hierarchicalCSBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) (*chunk.Chunk, error) {
	lookupKeys := getAllLookupKeys(blobDigest)
	canonicalKey := getCanonicalKey(blobDigest)

	for {
		// Look up the blob in storage while holding a read lock.
		ba.lock.RLock()
		_, location, err := ba.getLeastSpecificLookupEntry(lookupKeys)
		if err != nil {
			ba.lock.RUnlock()
			return nil, err
		}
		getter, needsRefresh := ba.locationBlobMap.Get(location)
		data, integrityCallback, err := getter(blobDigest)

		// We can now release the read lock and decode the data.
		ba.lock.RUnlock()
		if err != nil {
			return nil, err
		}
		val, err := ba.coder.Decode(data, blobDigest)
		if err != nil {
			integrityCallback()
			return nil, util.StatusWrapWithCode(err, codes.NotFound, "Blob did not decode when read")
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
		ba.lock.Lock()
		lookupKey, currentLocation, err := ba.getLeastSpecificLookupEntry(lookupKeys)
		if err != nil {
			ba.lock.Unlock()
			return nil, err
		}

		if currentLocation != location {
			// We came back from acquiring the lock and now the map
			// points to a new position in the block storage, the value
			// was either overwritten or refreshed by another thread. We
			// try again.
			ba.lock.Unlock()
			continue
		}

		// Maybe it already got refreshed as part of another instance
		// name prefix. First attempt to synchronize from the canonical
		// entry.
		if err := ba.syncFromCanonicalEntry(canonicalKey, lookupKey); err == nil {
			ba.lock.Unlock()
			return val, nil
		} else if status.Code(err) != codes.NotFound {
			ba.lock.Unlock()
			return nil, err
		}

		// Could not synchronize from the canonical entry. Allocate
		// space for a new copy.
		putWriter, err := ba.locationBlobMap.Put(currentLocation.SizeBytes)
		ba.lock.Unlock()
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to refresh blob")
		}

		putFinalizer := putWriter(data)

		ba.lock.Lock()
		err = ba.finalizePut(putFinalizer, canonicalKey, lookupKey)
		ba.lock.Unlock()
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to refresh blob")
		}
		return val, nil
	}
}

func (ba *hierarchicalCSBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, value *chunk.Chunk) error {
	// Encode data up front lock-free.
	data, err := ba.coder.Encode(value, blobDigest)
	if err != nil {
		return err
	}

	// Check whether the object has already been written to storage
	// under another instance name. In that case we don't want to
	// store a second copy.
	canonicalKey := getCanonicalKey(blobDigest)
	lookupKey := getMostSpecificLookupKey(blobDigest)
	ba.lock.Lock()
	if location, err := ba.keyLocationMap.Get(canonicalKey, ba.blockReferenceResolver); err == nil {
		if _, needsRefresh := ba.locationBlobMap.Get(location); !needsRefresh {
			// Create a new key-location map entry pointing
			// to the existing object.
			err = ba.keyLocationMap.Put(lookupKey, location, ba.blockReferenceResolver)
			ba.lock.Unlock()
			return err
		}
	} else if status.Code(err) != codes.NotFound {
		ba.lock.Unlock()
		return err
	}

	// Object not found, or it's close to expiring. Allocate space
	// for a new copy.
	putWriter, err := ba.locationBlobMap.Put(int64(len(data)))
	ba.lock.Unlock()
	if err != nil {
		return err
	}

	// Ingest the data associated with the object. This must be done
	// without holding any locks, so that I/O can happen in
	// parallel.
	putFinalizer := putWriter(data)

	// Write the object into the key-location map twice. Once with
	// the instance name and once without.
	ba.lock.Lock()
	defer ba.lock.Unlock()
	return ba.finalizePut(putFinalizer, canonicalKey, lookupKey)
}

func (ba *hierarchicalCSBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Convert all digests to a list of potential Keys.
	// TODO: This may be expensive to do all up front. Would it be
	// smarter to do this level by level? On the other hand, this
	// requires us to constantly relock. It also makes it harder to
	// reuse keys between the scanning and refreshing stages.
	allLookupKeys := make([][]Key, 0, digests.Length())
	for _, blobDigest := range digests.Items() {
		allLookupKeys = append(allLookupKeys, getAllLookupKeys(blobDigest))
	}

	type blobToRefresh struct {
		digest     digest.Digest
		lookupKeys []Key
	}
	var blobsToRefresh []blobToRefresh
	missing := digest.NewSetBuilder(0)
	ba.lock.RLock()
	for i, blobDigest := range digests.Items() {
		lookupKeys := allLookupKeys[i]
		if _, location, err := ba.getLeastSpecificLookupEntry(lookupKeys); err == nil {
			if _, needsRefresh := ba.locationBlobMap.Get(location); needsRefresh {
				// Blob is present, but it must be
				// refreshed for it to remain present.
				// Enqueue it for the second scan.
				blobsToRefresh = append(blobsToRefresh, blobToRefresh{
					digest:     blobDigest,
					lookupKeys: lookupKeys,
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

	canonicalKeys := make([]Key, 0, len(blobsToRefresh))
	for _, blobToRefresh := range blobsToRefresh {
		canonicalKeys = append(canonicalKeys, getCanonicalKey(blobToRefresh.digest))
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

	for i, blobToRefresh := range blobsToRefresh {
		ba.lock.RLock()
		_, location, err := ba.getLeastSpecificLookupEntry(blobToRefresh.lookupKeys)
		if err != nil {
			ba.lock.RUnlock()
			if status.Code(err) == codes.NotFound {
				// Blob disappeared between the first and second
				// scan. Simply report it as missing.
				missing.Add(blobToRefresh.digest)
				continue
			}
			return digest.EmptySet, util.StatusWrapf(err, "Failed to get blob %#v", blobToRefresh.digest.String())
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

		// Acquire write lock to sync or rewrite.
		ba.lock.Lock()
		lookupKey, currentLocation, err := ba.getLeastSpecificLookupEntry(blobToRefresh.lookupKeys)
		if err != nil {
			ba.lock.Unlock()
			if status.Code(err) == codes.NotFound {
				// Blob is missing after acquiring write lock.
				missing.Add(blobToRefresh.digest)
				continue
			}
			return digest.EmptySet, util.StatusWrapf(err, "Failed to get blob %#v", blobToRefresh.digest.String())
		}

		// Maybe it already got refreshed as part of another instance
		// name prefix. First attempt to synchronize from the canonical
		// entry.
		canonicalKey := canonicalKeys[i]
		if err := ba.syncFromCanonicalEntry(canonicalKey, lookupKey); err == nil {
			ba.lock.Unlock()
			continue
		} else if status.Code(err) != codes.NotFound {
			ba.lock.Unlock()
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}

		// Could not synchronize from the canonical entry. Allocate
		// space for a new copy.
		putWriter, err := ba.locationBlobMap.Put(currentLocation.SizeBytes)
		ba.lock.Unlock()

		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}

		// Copy the data while unlocked, so that concurrent requests for
		// other data continue to be serviced.
		putFinalizer := putWriter(data)

		ba.lock.Lock()
		if err := ba.finalizePut(putFinalizer, canonicalKey, lookupKey); err != nil {
			ba.lock.Unlock()
			return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
		}
		ba.lock.Unlock()
	}
	return missing.Build(), nil
}
