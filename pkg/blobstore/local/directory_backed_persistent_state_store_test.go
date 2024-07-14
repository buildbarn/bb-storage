package local_test

import (
	"io"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
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
		directory.EXPECT().OpenRead(path.MustNewComponent("state")).Return(nil, syscall.ENOENT)

		persistentState, err := persistentStateStore.ReadPersistentState()
		require.NoError(t, err)
		require.Equal(t, uint32(1), persistentState.OldestEpochId)
		require.Empty(t, persistentState.Blocks)
	})

	t.Run("ReadOpenFailure", func(t *testing.T) {
		directory.EXPECT().OpenRead(path.MustNewComponent("state")).Return(nil, syscall.EIO)

		_, err := persistentStateStore.ReadPersistentState()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to open file: input/output error"), err)
	})

	t.Run("ReadReadFailure", func(t *testing.T) {
		f := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead(path.MustNewComponent("state")).Return(f, nil)
		f.EXPECT().ReadAt(gomock.Any(), gomock.Any()).Return(0, syscall.EIO)
		f.EXPECT().Close()

		_, err := persistentStateStore.ReadPersistentState()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to read from file: input/output error"), err)
	})

	t.Run("ReadCorrupted", func(t *testing.T) {
		f := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead(path.MustNewComponent("state")).Return(f, nil)
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
		directory.EXPECT().OpenRead(path.MustNewComponent("state")).Return(f, nil)
		f.EXPECT().ReadAt(gomock.Any(), gomock.Any()).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, examplePersistentStateBytes), io.EOF
		})
		f.EXPECT().Close()

		persistentState, err := persistentStateStore.ReadPersistentState()
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &examplePersistentState, persistentState)
	})

	t.Run("WriteTemporaryFileRemovalFailure", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new")).Return(syscall.EACCES)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to remove previous temporary file: permission denied"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileCreationFailure", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new")).Return(syscall.ENOENT)
		directory.EXPECT().OpenAppend(path.MustNewComponent("state.new"), filesystem.CreateExcl(0o666)).Return(nil, syscall.EIO)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to create temporary file: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileWriteFailure", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new")).Return(syscall.ENOENT)
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend(path.MustNewComponent("state.new"), filesystem.CreateExcl(0o666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(0, syscall.ENOSPC)
		f.EXPECT().Close()

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to write to temporary file: no space left on device"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileSyncFailure", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new")).Return(syscall.ENOENT)
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend(path.MustNewComponent("state.new"), filesystem.CreateExcl(0o666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync().Return(syscall.EIO)
		f.EXPECT().Close()

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to synchronize temporary file: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteTemporaryFileCloseFailure", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new")).Return(syscall.ENOENT)
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend(path.MustNewComponent("state.new"), filesystem.CreateExcl(0o666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close().Return(syscall.EIO)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to close temporary file: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteDirectoryRenameFailure", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new"))
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend(path.MustNewComponent("state.new"), filesystem.CreateExcl(0o666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close()
		directory.EXPECT().Rename(path.MustNewComponent("state.new"), directory, path.MustNewComponent("state")).Return(syscall.EACCES)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to rename temporary file: permission denied"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteDirectorySyncFailure", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new"))
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend(path.MustNewComponent("state.new"), filesystem.CreateExcl(0o666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close()
		directory.EXPECT().Rename(path.MustNewComponent("state.new"), directory, path.MustNewComponent("state"))
		directory.EXPECT().Sync().Return(syscall.EIO)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to synchronize directory: input/output error"),
			persistentStateStore.WritePersistentState(&examplePersistentState))
	})

	t.Run("WriteSuccess", func(t *testing.T) {
		directory.EXPECT().Remove(path.MustNewComponent("state.new"))
		f := mock.NewMockFileAppender(ctrl)
		directory.EXPECT().OpenAppend(path.MustNewComponent("state.new"), filesystem.CreateExcl(0o666)).Return(f, nil)
		f.EXPECT().Write(examplePersistentStateBytes).Return(len(examplePersistentStateBytes), nil)
		f.EXPECT().Sync()
		f.EXPECT().Close()
		directory.EXPECT().Rename(path.MustNewComponent("state.new"), directory, path.MustNewComponent("state"))
		directory.EXPECT().Sync()

		require.NoError(t, persistentStateStore.WritePersistentState(&examplePersistentState))
	})
}
