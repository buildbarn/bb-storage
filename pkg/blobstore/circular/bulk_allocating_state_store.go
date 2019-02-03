package circular

type bulkAllocatingStateStore struct {
	StateStore
	chunkSize   uint64
	writeCursor uint64
}

// NewBulkAllocatingStateStore is an adapter for StateStore that reduces
// the number of Allocate() calls on the underlying implementation by
// allocating data as larger chunks. These chunks are then sub-allocated
// as needed.
func NewBulkAllocatingStateStore(stateStore StateStore, chunkSize uint64) StateStore {
	return &bulkAllocatingStateStore{
		StateStore:  stateStore,
		chunkSize:   chunkSize,
		writeCursor: ^uint64(0),
	}
}

func (ss *bulkAllocatingStateStore) Allocate(sizeBytes int64) (uint64, error) {
	// Move internal write cursor back in range. It may be out of
	// range initially, but also after Invalidate() calls.
	cursors := ss.GetCursors()
	if ss.writeCursor < cursors.Read {
		ss.writeCursor = cursors.Read
	}
	if ss.writeCursor > cursors.Write {
		ss.writeCursor = cursors.Write
	}

	// Allocate more space if needed.
	spaceLeft := cursors.Write - ss.writeCursor
	if uint64(sizeBytes) > spaceLeft {
		spaceMissing := uint64(sizeBytes) - spaceLeft
		allocationSize := (spaceMissing + ss.chunkSize - 1) / ss.chunkSize * ss.chunkSize
		if _, err := ss.StateStore.Allocate(int64(allocationSize)); err != nil {
			return 0, err
		}
	}

	// Perform allocation from current chunk.
	offset := ss.writeCursor
	ss.writeCursor += uint64(sizeBytes)
	return offset, nil
}
