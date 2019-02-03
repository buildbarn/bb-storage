package circular

import (
	"encoding/binary"

	"github.com/buildbarn/bb-storage/pkg/util"
)

type cachedRecord struct {
	digest simpleDigest
	offset uint64
	length int64
}

type cachingOffsetStore struct {
	backend OffsetStore
	table   []cachedRecord
}

// NewCachingOffsetStore is an adapter for OffsetStore that caches
// digests returned by/provided to previous calls of Get() and Put().
// Cached entries are stored in a fixed-size hash table.
//
// The purpose of this adapter is to significantly reduce the number of
// read operations on underlying storage. In the end it should reduce
// the running time of FindMissing() operations.
//
// TODO(edsch): Should we add negative caching as well?
func NewCachingOffsetStore(backend OffsetStore, size uint) OffsetStore {
	return &cachingOffsetStore{
		backend: backend,
		table:   make([]cachedRecord, size),
	}
}

func (os *cachingOffsetStore) Get(digest *util.Digest, cursors Cursors) (uint64, int64, bool, error) {
	simpleDigest := newSimpleDigest(digest)
	slot := binary.LittleEndian.Uint32(simpleDigest[:]) % uint32(len(os.table))
	foundRecord := os.table[slot]
	if foundRecord.digest == simpleDigest && cursors.Contains(foundRecord.offset, foundRecord.length) {
		return foundRecord.offset, foundRecord.length, true, nil
	}

	offset, length, found, err := os.backend.Get(digest, cursors)
	if err == nil && found {
		os.table[slot] = cachedRecord{
			digest: simpleDigest,
			offset: offset,
			length: length,
		}
	}
	return offset, length, found, err
}

func (os *cachingOffsetStore) Put(digest *util.Digest, offset uint64, length int64, cursors Cursors) error {
	if err := os.backend.Put(digest, offset, length, cursors); err != nil {
		return err
	}

	simpleDigest := newSimpleDigest(digest)
	slot := binary.LittleEndian.Uint32(simpleDigest[:]) % uint32(len(os.table))
	os.table[slot] = cachedRecord{
		digest: simpleDigest,
		offset: offset,
		length: length,
	}
	return nil
}
