package buffer_test

import (
	"bytes"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestCASBufferWithBackgroundTaskGetSizeBytes(t *testing.T) {
	done := make(chan struct{})
	b := buffer.NewCASBufferFromReader(
		digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "bc6e6f16b8a077ef5fbc8d59d0b931b9", 12),
		io.NopCloser(bytes.NewBufferString("Hello, world")),
		buffer.UserProvided,
	).WithTask(func() error {
		<-done
		return nil
	})

	// The size should be obtainable without waiting for the
	// background task to finish.
	sizeBytes, err := b.GetSizeBytes()
	require.NoError(t, err)
	require.Equal(t, int64(12), sizeBytes)

	close(done)
	b.Discard()
}

func TestCASBufferWithBackgroundTaskToByteSlice(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		b := buffer.NewCASBufferFromReader(
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "bc6e6f16b8a077ef5fbc8d59d0b931b9", 12),
			io.NopCloser(bytes.NewBufferString("Hello, world")),
			buffer.UserProvided,
		).WithTask(func() error { return nil })

		data, err := b.ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, world"), data)
	})

	t.Run("Failure", func(t *testing.T) {
		b := buffer.NewCASBufferFromReader(
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "bc6e6f16b8a077ef5fbc8d59d0b931b9", 12),
			io.NopCloser(bytes.NewBufferString("Hello, world")),
			buffer.UserProvided,
		).WithTask(func() error { return status.Error(codes.Internal, "Synchronization failed") })

		_, err := b.ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Synchronization failed"), err)
	})
}

func TestCASBufferWithBackgroundTaskToChunkReader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		b := buffer.NewCASBufferFromReader(
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "bc6e6f16b8a077ef5fbc8d59d0b931b9", 12),
			io.NopCloser(bytes.NewBufferString("Hello, world")),
			buffer.UserProvided,
		).WithTask(func() error { return nil })

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
		b := buffer.NewCASBufferFromReader(
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
			io.NopCloser(bytes.NewBufferString("Hello")),
			buffer.UserProvided,
		).WithTask(func() error { return status.Error(codes.Internal, "Synchronization failed") })

		// Because ChunkReader.Close() does not return any
		// errors, the io.EOF should be replaced with the error
		// of the background task.
		r := b.ToChunkReader(0, 5)
		data, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		_, err = r.Read()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Synchronization failed"), err)
		_, err = r.Read()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Synchronization failed"), err)
		r.Close()
	})
}

func TestCASBufferWithBackgroundTaskToReader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		b := buffer.NewCASBufferFromReader(
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "bc6e6f16b8a077ef5fbc8d59d0b931b9", 12),
			io.NopCloser(bytes.NewBufferString("Hello, world")),
			buffer.UserProvided,
		).WithTask(func() error { return nil })

		r := b.ToReader()
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, world"), data)
		require.NoError(t, r.Close())
	})

	t.Run("Failure", func(t *testing.T) {
		b := buffer.NewCASBufferFromReader(
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
			io.NopCloser(bytes.NewBufferString("Hello")),
			buffer.UserProvided,
		).WithTask(func() error { return status.Error(codes.Internal, "Synchronization failed") })

		// io.ReadCloser.Close() is used to return errors of
		// background tasks.
		r := b.ToReader()
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Synchronization failed"), r.Close())
	})
}

func TestCASBufferWithBackgroundTaskWithErrorHandler(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Create a buffer that has both a background task and an error
	// handler attached. The error of the background task should be
	// shadowed by the error returned in the foreground, and should
	// not be returned to the caller.
	r := mock.NewMockReadCloser(ctrl)
	r.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.NotFound, "Object not found"))
	r.EXPECT().Close()
	b := buffer.NewCASBufferFromReader(
		digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "bc6e6f16b8a077ef5fbc8d59d0b931b9", 12),
		r,
		buffer.UserProvided,
	).WithTask(func() error { return status.Error(codes.Internal, "Synchronization failed") })

	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.NotFound, "Object not found")).
		Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello, world")), nil)
	errorHandler.EXPECT().Done()
	b = buffer.WithErrorHandler(b, errorHandler)

	data, err := b.ToByteSlice(10000)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello, world"), data)
}
