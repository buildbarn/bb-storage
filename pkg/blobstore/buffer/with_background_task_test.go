package buffer_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWithBackgroundTaskGetSizeBytes(t *testing.T) {
	b, task := buffer.WithBackgroundTask(buffer.NewValidatedBufferFromByteSlice([]byte("Hello, world")))

	// The size should be obtainable without waiting for the
	// background task to finish.
	sizeBytes, err := b.GetSizeBytes()
	require.NoError(t, err)
	require.Equal(t, int64(12), sizeBytes)

	task.Finish(nil)
	b.Discard()
}

func TestWithBackgroundTaskToByteSlice(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		b, task := buffer.WithBackgroundTask(buffer.NewValidatedBufferFromByteSlice([]byte("Hello, world")))
		task.Finish(nil)

		data, err := b.ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, world"), data)
	})

	t.Run("Failure", func(t *testing.T) {
		b, task := buffer.WithBackgroundTask(buffer.NewValidatedBufferFromByteSlice([]byte("Hello, world")))
		task.Finish(status.Error(codes.Internal, "Synchronization failed"))

		_, err := b.ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Synchronization failed"), err)
	})
}

func TestWithBackgroundTaskToChunkReader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		b, task := buffer.WithBackgroundTask(buffer.NewValidatedBufferFromByteSlice([]byte("Hello, world")))
		task.Finish(nil)

		r := b.ToChunkReader(0, 5)
		data, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		data, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte(", wor"), data)
		data, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("ld"), data)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("Failure", func(t *testing.T) {
		b, task := buffer.WithBackgroundTask(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		task.Finish(status.Error(codes.Internal, "Synchronization failed"))

		// Because ChunkReader.Close() does not return any
		// errors, the io.EOF should be replaced with the error
		// of the background task.
		r := b.ToChunkReader(0, 5)
		data, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		_, err = r.Read()
		require.Equal(t, status.Error(codes.Internal, "Synchronization failed"), err)
		_, err = r.Read()
		require.Equal(t, status.Error(codes.Internal, "Synchronization failed"), err)
		r.Close()
	})
}

func TestWithBackgroundTaskToReader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		b, task := buffer.WithBackgroundTask(buffer.NewValidatedBufferFromByteSlice([]byte("Hello, world")))
		task.Finish(nil)

		r := b.ToReader()
		data, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, world"), data)
		require.NoError(t, r.Close())
	})

	t.Run("Failure", func(t *testing.T) {
		b, task := buffer.WithBackgroundTask(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		task.Finish(status.Error(codes.Internal, "Synchronization failed"))

		// io.ReadCloser.Close() is used to return errors of
		// background tasks.
		r := b.ToReader()
		data, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		require.Equal(t, status.Error(codes.Internal, "Synchronization failed"), r.Close())
	})
}
