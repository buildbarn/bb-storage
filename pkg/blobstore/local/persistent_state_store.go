package local

import (
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
)

// PersistentStateStore is used by PeriodicSyncer to write
// PersistentBlockList's state to disk. This state can be reloaded on
// startup to make it possible to access data that was written in the
// past.
type PersistentStateStore interface {
	ReadPersistentState() (*pb.PersistentState, error)
	WritePersistentState(persistentState *pb.PersistentState) error
}
