package local

import (
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

var (
	componentState    = path.MustNewComponent("state")
	componentStateNew = path.MustNewComponent("state.new")
)

type directoryBackedPersistentStateStore struct {
	directory filesystem.Directory
}

// NewDirectoryBackedPersistentStateStore creates a PersistentStateStore
// that writes PersistentState Protobuf messages to a file named "state"
// stored inside a filesystem.Directory.
func NewDirectoryBackedPersistentStateStore(directory filesystem.Directory) PersistentStateStore {
	return directoryBackedPersistentStateStore{
		directory: directory,
	}
}

func newPersistentState() *pb.PersistentState {
	return &pb.PersistentState{
		OldestEpochId:                    1,
		KeyLocationMapHashInitialization: random.CryptoThreadSafeGenerator.Uint64(),
	}
}

func (pss directoryBackedPersistentStateStore) ReadPersistentState() (*pb.PersistentState, error) {
	f, err := pss.directory.OpenRead(componentState)
	if os.IsNotExist(err) {
		// No state file present. Reinitialize the data store.
		log.Print("Reinitializing data store, as persistent state was not found")
		return newPersistentState(), nil
	}
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to open file")
	}
	defer f.Close()

	data, err := ioutil.ReadAll(io.NewSectionReader(f, 0, math.MaxInt64))
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to read from file")
	}
	var persistentState pb.PersistentState
	if err := proto.Unmarshal(data, &persistentState); err != nil {
		// The state file was read successfully, but we were
		// unable to find any usable state. As this is not a
		// transient issue, let's reinitialize so that the
		// system doesn't remain in a broken state.
		log.Print("Reinitializing data store, as persistent state was corrupted")
		return newPersistentState(), nil
	}
	return &persistentState, nil
}

func (pss directoryBackedPersistentStateStore) WritePersistentState(persistentState *pb.PersistentState) error {
	// Marshal the persistent state.
	data, err := proto.Marshal(persistentState)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to marshal data")
	}

	// Write the persistent state to a temporary file.
	if err := pss.directory.Remove(componentStateNew); err != nil && !os.IsNotExist(err) {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to remove previous temporary file")
	}
	f, err := pss.directory.OpenAppend(componentStateNew, filesystem.CreateExcl(0o666))
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create temporary file")
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to write to temporary file")
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to synchronize temporary file")
	}
	if err := f.Close(); err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to close temporary file")
	}

	// Move the new persistent state over the old copy.
	if err := pss.directory.Rename(componentStateNew, pss.directory, componentState); err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to rename temporary file")
	}
	if err := pss.directory.Sync(); err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to synchronize directory")
	}
	return nil
}
