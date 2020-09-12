package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
)

type locationBasedKeyBlobMap struct {
	keyLocationMap  KeyLocationMap
	locationBlobMap LocationBlobMap
}

// NewLocationBasedKeyBlobMap creates a KeyBlobMap (map[Key][]byte) that
// is implemented by chaining requests through a KeyLocationMap
// (map[Key]Location) and a LocationBlobMap (map[Location][]byte).
func NewLocationBasedKeyBlobMap(keyLocationMap KeyLocationMap, locationBlobMap LocationBlobMap) KeyBlobMap {
	return &locationBasedKeyBlobMap{
		keyLocationMap:  keyLocationMap,
		locationBlobMap: locationBlobMap,
	}
}

func (kbm *locationBasedKeyBlobMap) Get(key Key) (KeyBlobGetter, int64, bool, error) {
	// Obtain the blob's location from the KeyLocationMap. The
	// location that is returned, is guaranteed to be valid. The
	// BlockReferenceResolver makes sure of that.
	location, err := kbm.keyLocationMap.Get(key)
	if err != nil {
		return nil, 0, false, err
	}

	blobGetter, needsRefresh := kbm.locationBlobMap.Get(location)
	return KeyBlobGetter(blobGetter), location.SizeBytes, needsRefresh, nil
}

func (kbm *locationBasedKeyBlobMap) Put(sizeBytes int64) (KeyBlobPutWriter, error) {
	// Allocate space while holding a lock.
	putWriter, err := kbm.locationBlobMap.Put(sizeBytes)
	if err != nil {
		return nil, err
	}

	return func(b buffer.Buffer) KeyBlobPutFinalizer {
		// Copy data without having a lock held.
		putFinalizer := putWriter(b)

		return func(key Key) error {
			// Write an entry into the KeyLocationMap after
			// successfully writing data into the
			// LocationBlobMap, so that the blob can be
			// reobtained.
			location, err := putFinalizer()
			if err != nil {
				return err
			}
			return kbm.keyLocationMap.Put(key, location)
		}
	}, nil
}
