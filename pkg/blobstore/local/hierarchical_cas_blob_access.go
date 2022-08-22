package local

import (
	"context"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hierarchicalCASBlobAccess struct {
	capabilities.Provider

	keyLocationMap  KeyLocationMap
	locationBlobMap LocationBlobMap

	lock        *sync.RWMutex
	refreshLock sync.Mutex
}

// NewHierarchicalCASBlobAccess creates a BlobAccess that uses a
// KeyLocationMap and a LocationBlobMap as backing stores.
//
// The BlobAccess returned by this function can be thought of as being
// an amalgamation of FlatBlobAccess and LocationBasedKeyBlobMap, with
// one big difference: it keeps track of which REv2 instance name
// prefixes are permitted to access an object. It does this by writing
// multiple entries into the key-location map:
//
//   - One canonical entry that always points to the newest copy of an
//     object. This entry's key does not contain an instance name.
//   - One or more lookup entries, whose keys contain an instance name
//     prefix. These are only synchronized with the canonical entry when
//     the lookup entry points to an object that needs to be refreshed.
//
// As the name implies, this implementation should only be used for the
// Content Addressable Storage (CAS). This is because writes for objects
// that already exist for a different REv2 instance name don't cause any
// new data to be ingested. This makes this implementation unsuitable
// for mutable data sets.
func NewHierarchicalCASBlobAccess(keyLocationMap KeyLocationMap, locationBlobMap LocationBlobMap, lock *sync.RWMutex, capabilitiesProvider capabilities.Provider) blobstore.BlobAccess {
	return &hierarchicalCASBlobAccess{
		Provider: capabilitiesProvider,

		keyLocationMap:  keyLocationMap,
		locationBlobMap: locationBlobMap,
		lock:            lock,
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

// getLeastSpecificLookupEntry searches the key-location for an object,
// given a list of lookup Keys. It returns the first Key (with the
// shortest instance name) for which a match occurred, together with a
// Location at which the object is stored.
func (ba *hierarchicalCASBlobAccess) getLeastSpecificLookupEntry(lookupKeys []Key) (Key, Location, error) {
	for _, lookupKey := range lookupKeys {
		if location, err := ba.keyLocationMap.Get(lookupKey); err == nil {
			return lookupKey, location, nil
		} else if status.Code(err) != codes.NotFound {
			return Key{}, Location{}, err
		}
	}
	return Key{}, Location{}, status.Error(codes.NotFound, "Object not found")
}

// syncFromCanonicalEntry attempts to synchronize a lookup entry in the
// key-location map to point to the canonical version of an object, if
// it exists and doesn't need to be refreshed.
//
// This method can be used to refresh a key-location map without
// necessarily copying the data of the underlying object.
func (ba *hierarchicalCASBlobAccess) syncFromCanonicalEntry(canonicalKey, lookupKey Key) (LocationBlobGetter, error) {
	canonicalLocation, err := ba.keyLocationMap.Get(canonicalKey)
	if err != nil {
		return nil, err
	}
	getter, needsRefresh := ba.locationBlobMap.Get(canonicalLocation)
	if needsRefresh {
		return nil, status.Error(codes.NotFound, "Canonical entry needs to be refreshed")
	}
	return getter, ba.keyLocationMap.Put(lookupKey, canonicalLocation)
}

// finalizePut is called to finalize a write to the data store. This
// method must be called while holding the write lock.
func (ba *hierarchicalCASBlobAccess) finalizePut(putFinalizer LocationBlobPutFinalizer, canonicalKey, lookupKey Key) error {
	// Finalize the write of the data.
	location, err := putFinalizer()
	if err != nil {
		return err
	}

	// Store two key-location map entries: one for the canonical key
	// and one for the lookup key.
	if err := ba.keyLocationMap.Put(canonicalKey, location); err != nil {
		return err
	}
	return ba.keyLocationMap.Put(lookupKey, location)
}

func (ba *hierarchicalCASBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	lookupKeys := getAllLookupKeys(blobDigest)

	// Look up the blob in storage while holding a read lock.
	ba.lock.RLock()
	_, location, err := ba.getLeastSpecificLookupEntry(lookupKeys)
	if err != nil {
		ba.lock.RUnlock()
		return buffer.NewBufferFromError(err)
	}
	if getter, needsRefresh := ba.locationBlobMap.Get(location); !needsRefresh {
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
	canonicalKey := getCanonicalKey(blobDigest)
	ba.lock.Lock()
	lookupKey, lookupLocation, err := ba.getLeastSpecificLookupEntry(lookupKeys)
	if err != nil {
		ba.lock.Unlock()
		return buffer.NewBufferFromError(err)
	}
	getter, needsRefresh := ba.locationBlobMap.Get(lookupLocation)
	if !needsRefresh {
		// Some other thread managed to refresh the blob before
		// we got the write lock. No need to copy anymore.
		b := getter(blobDigest)
		ba.lock.Unlock()
		return b
	}

	// Maybe it already got refreshed as part of another instance
	// name prefix. First attempt to synchronize from the canonical
	// entry.
	if getter, err := ba.syncFromCanonicalEntry(canonicalKey, lookupKey); err == nil {
		b := getter(blobDigest)
		ba.lock.Unlock()
		return b
	} else if status.Code(err) != codes.NotFound {
		ba.lock.Unlock()
		return buffer.NewBufferFromError(err)
	}

	// Could not synchronize from the canonical entry. Allocate
	// space for a new copy.
	b := getter(blobDigest)
	putWriter, err := ba.locationBlobMap.Put(lookupLocation.SizeBytes)
	ba.lock.Unlock()
	if err != nil {
		return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to refresh blob"))
	}

	// Copy the object while it's been returned. Block until copying
	// has finished to apply back-pressure.
	b1, b2 := b.CloneStream()
	return b1.WithTask(func() error {
		putFinalizer := putWriter(b2)
		ba.lock.Lock()
		err := ba.finalizePut(putFinalizer, canonicalKey, lookupKey)
		ba.lock.Unlock()
		if err != nil {
			return util.StatusWrap(err, "Failed to refresh blob")
		}
		return nil
	})
}

func (ba *hierarchicalCASBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}

	// Check whether the object has already been written to storage
	// under another instance name. In that case we don't want to
	// store a second copy.
	canonicalKey := getCanonicalKey(blobDigest)
	lookupKey := getMostSpecificLookupKey(blobDigest)
	ba.lock.Lock()
	if location, err := ba.keyLocationMap.Get(canonicalKey); err == nil {
		if _, needsRefresh := ba.locationBlobMap.Get(location); !needsRefresh {
			ba.lock.Unlock()

			// Do make sure that the caller actually
			// provided a valid copy of the data, as we
			// don't want to allow the client to gain access
			// to an object it doesn't possess. The buffer
			// layer validates data automatically, so we
			// only need to consume the buffer.
			if err := b.IntoWriter(io.Discard); err != nil {
				return err
			}

			// Create a new key-location map entry pointing
			// to the existing object. We can't use the
			// location read previously, as dropping the
			// lock invalidated it.
			ba.lock.Lock()
			defer ba.lock.Unlock()
			location, err := ba.keyLocationMap.Get(canonicalKey)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					return status.Error(codes.Internal, "Existing object disappeared while buffer was read")
				}
				return err
			}
			return ba.keyLocationMap.Put(lookupKey, location)
		}
	} else if status.Code(err) != codes.NotFound {
		ba.lock.Unlock()
		b.Discard()
		return err
	}

	// Object not found, or it's close to expiring. Allocate space
	// for a new copy.
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

	// Write the object into the key-location map twice. Once with
	// the instance name and once without.
	ba.lock.Lock()
	defer ba.lock.Unlock()
	return ba.finalizePut(putFinalizer, canonicalKey, lookupKey)
}

func (ba *hierarchicalCASBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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
	missing := digest.NewSetBuilder()
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

	ba.lock.Lock()
	for i, blobToRefresh := range blobsToRefresh {
		if lookupKey, lookupLocation, err := ba.getLeastSpecificLookupEntry(blobToRefresh.lookupKeys); err == nil {
			if getter, needsRefresh := ba.locationBlobMap.Get(lookupLocation); needsRefresh {
				// Maybe it already got refreshed as
				// part of another instance name prefix.
				// First attempt to synchronize from the
				// canonical entry.
				canonicalKey := canonicalKeys[i]
				if _, err := ba.syncFromCanonicalEntry(canonicalKey, lookupKey); err == nil {
					continue
				} else if status.Code(err) != codes.NotFound {
					ba.lock.Unlock()
					return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
				}

				// Could not synchronize from the
				// canonical entry. Allocate space for a
				// new copy.
				b := getter(blobToRefresh.digest)
				putWriter, err := ba.locationBlobMap.Put(lookupLocation.SizeBytes)
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
				if err := ba.finalizePut(putFinalizer, canonicalKey, lookupKey); err != nil {
					ba.lock.Unlock()
					return digest.EmptySet, util.StatusWrapf(err, "Failed to refresh blob %#v", blobToRefresh.digest.String())
				}
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
	return missing.Build(), nil
}
