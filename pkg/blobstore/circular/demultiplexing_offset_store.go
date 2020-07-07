package circular

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// OffsetStoreGetter is the callback type used by the demultiplexing
// offset store and is invoked for every operation.
type OffsetStoreGetter func(instanceName string) (OffsetStore, error)

type demultiplexingOffsetStore struct {
	offsetStoreGetter OffsetStoreGetter
}

// NewDemultiplexingOffsetStore creates an OffsetStore that
// demultiplexes operations based on the instance name stored in the
// provided digest. This may be used for Action Cache purposes, where a
// single storage server may be used to store cached actions for
// multiple instance names.
func NewDemultiplexingOffsetStore(offsetStoreGetter OffsetStoreGetter) OffsetStore {
	return &demultiplexingOffsetStore{
		offsetStoreGetter: offsetStoreGetter,
	}
}

func (os *demultiplexingOffsetStore) Get(digest digest.Digest, cursors Cursors) (uint64, int64, bool, error) {
	instance := digest.GetInstanceName().String()
	backend, err := os.offsetStoreGetter(instance)
	if err != nil {
		return 0, 0, false, util.StatusWrapf(err, "Failed to obtain offset store for instance %#v", instance)
	}
	return backend.Get(digest, cursors)
}

func (os *demultiplexingOffsetStore) Put(digest digest.Digest, offset uint64, length int64, cursors Cursors) error {
	instance := digest.GetInstanceName().String()
	backend, err := os.offsetStoreGetter(instance)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain offset store for instance %#v", instance)
	}
	return backend.Put(digest, offset, length, cursors)
}
