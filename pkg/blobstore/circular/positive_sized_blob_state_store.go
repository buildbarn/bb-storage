package circular

type positiveSizedBlobStateStore struct {
	StateStore
}

// NewPositiveSizedBlobStateStore is an adapter for StateStore that
// forces allocations of blobs to be positive in size. Zero-sized
// allocations would permit multiple blobs to be stored at the same
// offset. Invalidations of such blobs would normally have no effect,
// causing the storage backend to be unable to repair itself.
func NewPositiveSizedBlobStateStore(stateStore StateStore) StateStore {
	return &positiveSizedBlobStateStore{
		StateStore: stateStore,
	}
}

func (ss *positiveSizedBlobStateStore) Allocate(sizeBytes int64) (uint64, error) {
	if sizeBytes < 1 {
		sizeBytes = 1
	}
	return ss.StateStore.Allocate(sizeBytes)
}

func (ss *positiveSizedBlobStateStore) Invalidate(offset uint64, sizeBytes int64) error {
	if sizeBytes < 1 {
		sizeBytes = 1
	}
	return ss.StateStore.Invalidate(offset, sizeBytes)
}
