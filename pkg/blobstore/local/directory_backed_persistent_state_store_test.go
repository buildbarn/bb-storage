package local_test

import (
	"io"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDirectoryBackedPersistentStateStore(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	persistentStateStore := local.NewDirectoryBackedPersistentStateStore(directory)

	// Example persistent state to read from/write to disk.
	examplePersistentState := pb.PersistentState{
		OldestEpochId:                    123,
		KeyLocationMapHashInitialization: 0xa0d1949bda40b526,
	}
	examplePersistentStateBytes := []byte{0x08, 0x7b, 0x18, 0xa6, 0xea, 0x82, 0xd2, 0xbd, 0x93, 0xe5, 0xe8, 0xa0, 0x01}

	t.Run("ReadNotFound", func(t *testing.T) {
		directory.EXPECT().OpenRead("state").Return(nil, syscall.ENOENT)

		persistentState, err := persistentStateStore.ReadPersistentState()
		require.NoError(t, err)
		require.Equal(t, uint32(1), persistentState.OldestEpochId)
		require.Empty(t, persistentState.Blocks)
	})

	t.Run("ReadOpenFailure", func(t *testing.T) {
		directory.EXPECT().OpenRead("state").Return(nil, syscall.EIO)

		_, err := persistentStateStore.ReadPersistentState()
		require.Equal(t, status.Error(codes.Internal, "Failed to open file: input/output error"), err)
	})

	t.Run("ReadReadFailure", func(t *testing.T) {
		f := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead("state").Return(f, nil)
		f.EXPECT().ReadAt(gomock.Any(), gomock.Any()).Return(0, syscall.EIO)
		f.EXPECT().Close()

		_, err := persistentStateStore.ReadPersistentState()
		require.Equal(t, status.Error(codes.Internal, "Failed to read from file: input/output error"), err)
	})

	t.Run("ReadCorrupted", func(t *testing.T) {
		f := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead("state").Return(f, nil)
		f.EXPECT().ReadAt(gomock.Any(), gomock.Any()).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, "This is not a valid protobuf"), io.EOF
		})
		f.EXPECT().Close()

		persistentState, err := persistentStateStore.ReadPersistentState()
		require.NoError(t, err)
		require.Equal(t, uint32(1), persistentState.OldestEpochId)
		require.Empty(t, persistentState.Blocks)
	})

	t.Run("ReadSuccess", func(t *testing.T) {
		f := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead("state").Return(f, nil)
		f.EXPECT().ReadAt(gomock.Any(), gomock.Any()).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, examplePersistentStateBytes), io.EOF
		})
		f.EXPECT().Close()

		persistentState, err := persistentStateStore.ReadPersistentState()
		require.NoError(t, err)
		require.True(t, proto.Equal(persistentState, &examplePersistentState))
	})

	t.Run("WriteTemporaryFileRemovalFailure", func(t *testing.T) {
		directory.EXPECT().Remove("state.new").Return(syscall.EACCES)

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to remove previous temporary file: permission denied"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileCreationFailure", func(t *testing.T) {
		directory.EXPECT().Remove("state.new").Return(syscall.ENOENT)
		directory.EXPECT().OpenAppend("state.new", filesystem.CreateExcl(0666)).Return(nil, syscall.EIO)

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to create temporary file: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileWriteFailure", func(t *testing.T) {
		directory.EXPECT().Remove("state.new").Return(syscall.ENOENT)
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend("state.new", filesystem.CreateExcl(0666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(0, syscall.ENOSPC)
		f.EXPECT().Close()

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to write to temporary file: no space left on device"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileSyncFailure", func(t *testing.T) {
		directory.EXPECT().Remove("state.new").Return(syscall.ENOENT)
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend("state.new", filesystem.CreateExcl(0666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync().Return(syscall.EIO)
		f.EXPECT().Close()

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to synchronize temporary file: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileCloseFailure", func(t *testing.T) {
		directory.EXPECT().Remove("state.new").Return(syscall.ENOENT)
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend("state.new", filesystem.CreateExcl(0666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close().Return(syscall.EIO)

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to close temporary file: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteDirectoryRenameFailure", func(t *testing.T) {
		directory.EXPECT().Remove("state.new")
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend("state.new", filesystem.CreateExcl(0666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close()
		directory.EXPECT().Rename("state.new", directory, "state").Return(syscall.EACCES)

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to rename temporary file: permission denied"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteDirectorySyncFailure", func(t *testing.T) {
		directory.EXPECT().Remove("state.new")
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend("state.new", filesystem.CreateExcl(0666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close()
		directory.EXPECT().Rename("state.new", directory, "state")
		directory.EXPECT().Sync().Return(syscall.EIO)

		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to synchronize directory: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteSuccess", func(t *testing.T) {
		directory.EXPECT().Remove("state.new")
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend("state.new", filesystem.CreateExcl(0666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close()
		directory.EXPECT().Rename("state.new", directory, "state")
		directory.EXPECT().Sync()

		require.NoError(t, persistentStateStore.WritePersistentState(&examplePersistentState))
	})
}
