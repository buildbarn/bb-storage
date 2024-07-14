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

func TestWithErrorHandlerOnProtoBuffers(t *testing.T) {
	ctrl := gomock.NewController(t)

	// For Protobuf backed buffers, there is no need to make a
	// distinction between operations in terms of error handling. As
	// none of the Protobuf backed buffer types are lazy loading,
	// WithErrorHandler() is always capable of evaluating the
	// ErrorHandler immediately. It is therefore sufficient to only
	// test ToByteSlice().

	t.Run("ImmediateSuccess", func(t *testing.T) {
		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().Done()

		data, err := buffer.WithErrorHandler(
			buffer.NewProtoBufferFromByteSlice(
				&remoteexecution.ActionResult{},
				exampleActionResultBytes,
				buffer.UserProvided),
			errorHandler).ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, data)
	})

	t.Run("RetriesFailed", func(t *testing.T) {
		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Network error"))).
			Return(buffer.NewProtoBufferFromByteSlice(
				&remoteexecution.ActionResult{},
				[]byte("Hello"),
				buffer.UserProvided), nil)
		errorHandler.EXPECT().OnError(testutil.EqPrefixedStatus(status.Error(codes.InvalidArgument, "Failed to unmarshal message: proto:"))).
			Return(nil, status.Error(codes.Internal, "Maximum number of retries reached"))
		errorHandler.EXPECT().Done()

		_, err := buffer.WithErrorHandler(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Network error")),
			errorHandler).ToByteSlice(1000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Maximum number of retries reached"), err)
	})

	t.Run("RetriesSuccess", func(t *testing.T) {
		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Network error"))).
			Return(buffer.NewProtoBufferFromByteSlice(
				&remoteexecution.ActionResult{},
				[]byte("Hello"),
				buffer.UserProvided), nil)
		errorHandler.EXPECT().OnError(testutil.EqPrefixedStatus(status.Error(codes.InvalidArgument, "Failed to unmarshal message: proto:"))).
			Return(buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided), nil)
		errorHandler.EXPECT().Done()

		data, err := buffer.WithErrorHandler(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Network error")),
			errorHandler).ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, data)
	})
}

func TestWithErrorHandlerOnSimpleCASBuffers(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Similar to the Protobuf backed buffer, buffers created
	// through NewCASBufferFrom{ByteSlice,Error}() also allow for
	// immediate ErrorHandler evaluation. It is sufficient to only
	// test ToByteSlice().

	digest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)

	t.Run("ImmediateSuccess", func(t *testing.T) {
		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().Done()

		data, err := buffer.WithErrorHandler(
			buffer.NewCASBufferFromByteSlice(digest, []byte("Hello world"), buffer.UserProvided),
			errorHandler).ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("RetriesFailed", func(t *testing.T) {
		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Network error")).
			Return(buffer.NewCASBufferFromByteSlice(digest, []byte("Hello"), buffer.UserProvided), nil)
		errorHandler.EXPECT().OnError(status.Error(codes.InvalidArgument, "Buffer is 5 bytes in size, while 11 bytes were expected")).
			Return(nil, status.Error(codes.Internal, "Maximum number of retries reached"))
		errorHandler.EXPECT().Done()

		_, err := buffer.WithErrorHandler(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Network error")),
			errorHandler).ToByteSlice(1000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Maximum number of retries reached"), err)
	})

	t.Run("RetriesSuccess", func(t *testing.T) {
		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Network error")).
			Return(buffer.NewCASBufferFromByteSlice(digest, []byte("Hello"), buffer.UserProvided), nil)
		errorHandler.EXPECT().OnError(status.Error(codes.InvalidArgument, "Buffer is 5 bytes in size, while 11 bytes were expected")).
			Return(buffer.NewCASBufferFromByteSlice(digest, []byte("Hello world"), buffer.UserProvided), nil)
		errorHandler.EXPECT().Done()

		data, err := buffer.WithErrorHandler(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Network error")),
			errorHandler).ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})
}

func TestWithErrorHandlerOnCASBuffersIntoWriter(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)

	t.Run("RetriesFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Hello "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(nil, status.Error(codes.Internal, "No backends available"))
		errorHandler.EXPECT().Done()

		writer := bytes.NewBuffer(nil)
		err := buffer.WithErrorHandler(b1, errorHandler).IntoWriter(writer)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "No backends available"), err)
		require.Equal(t, []byte("Hello "), writer.Bytes())
	})

	t.Run("RetriesSuccess", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Hello "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.UserProvided)

		reader2 := io.NopCloser(bytes.NewBufferString("XXXXXXworld"))
		b2 := buffer.NewCASBufferFromReader(digest, reader2, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		// The start of the second stream is discarded, as the
		// first stream was already written to the output. Data
		// corruption within a part of the stream that is
		// already written is not a problem.
		writer := bytes.NewBuffer(nil)
		err := buffer.WithErrorHandler(b1, errorHandler).IntoWriter(writer)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), writer.Bytes())
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Xyzzy "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		dataIntegrityCallback1 := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback1.EXPECT().Call(false)
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.BackendProvided(dataIntegrityCallback1.Call))

		reader2 := io.NopCloser(bytes.NewBufferString("Hello world"))
		dataIntegrityCallback2 := mock.NewMockDataIntegrityCallback(ctrl)
		b2 := buffer.NewCASBufferFromReader(digest, reader2, buffer.BackendProvided(dataIntegrityCallback2.Call))

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		// Merging the two streams will cause "Xyzzy world" to
		// be returned. This causes a checksum failure. There is
		// no way to recover from this, because we've already
		// written a part of the corrupted data into the output
		// stream.
		writer := bytes.NewBuffer(nil)
		err := buffer.WithErrorHandler(b1, errorHandler).IntoWriter(writer)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 3c61ab3f7343f99e0d18e0a7dfb3b0ce, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"), err)
		require.Equal(t, []byte("Xyzzy "), writer.Bytes())
	})
}

// Only provide simple testing coverage for ReadAt(). It is built on top
// of exactly the same retry logic as ToProto().
func TestWithErrorHandlerOnCASBuffersReadAt(t *testing.T) {
	ctrl := gomock.NewController(t)

	reader1 := mock.NewMockReadCloser(ctrl)
	reader1.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Internal, "Connection closed"))
	reader1.EXPECT().Close().Return(nil)
	b1 := buffer.NewCASBufferFromReader(exampleActionResultDigest, reader1, buffer.UserProvided)
	b2 := buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))

	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
	errorHandler.EXPECT().Done()

	var b [2]byte
	n, err := buffer.WithErrorHandler(b1, errorHandler).ReadAt(b[:], 2)
	require.Equal(t, 2, n)
	require.NoError(t, err)
}

func TestWithErrorHandlerOnCASBuffersToProto(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("RetriesFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return(exampleActionResultBytes[:10], nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(exampleActionResultDigest, reader1, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(nil, status.Error(codes.Internal, "No backends available"))
		errorHandler.EXPECT().Done()

		_, err := buffer.WithErrorHandler(b1, errorHandler).ToProto(&remoteexecution.ActionResult{}, 10000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "No backends available"), err)
	})

	t.Run("RetriesSuccess", func(t *testing.T) {
		reader1 := mock.NewMockReadCloser(ctrl)
		reader1.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close().Return(nil)
		b1 := buffer.NewCASBufferFromReader(exampleActionResultDigest, reader1, buffer.UserProvided)

		reader2 := mock.NewMockChunkReader(ctrl)
		reader2.EXPECT().Read().Return(exampleActionResultBytes, nil)
		reader2.EXPECT().Read().Return(nil, io.EOF)
		reader2.EXPECT().Close()
		b2 := buffer.NewCASBufferFromChunkReader(exampleActionResultDigest, reader2, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		actionResult, err := buffer.WithErrorHandler(b1, errorHandler).ToProto(&remoteexecution.ActionResult{}, 10000)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &exampleActionResultMessage, actionResult)
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Hello"), nil)
		reader1.EXPECT().Read().Return(nil, io.EOF)
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(exampleActionResultDigest, reader1, buffer.UserProvided)

		reader2 := mock.NewMockChunkReader(ctrl)
		reader2.EXPECT().Read().Return(exampleActionResultBytes, nil)
		reader2.EXPECT().Read().Return(nil, io.EOF)
		reader2.EXPECT().Close()
		b2 := buffer.NewCASBufferFromChunkReader(exampleActionResultDigest, reader2, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.InvalidArgument, "Buffer is 5 bytes in size, while 134 bytes were expected")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		// Operations like ToProto() may be safely retried, even
		// in the case of data inconsistency errors. The call
		// should succeed, even if it obtained invalid data
		// initially.
		actionResult, err := buffer.WithErrorHandler(b1, errorHandler).ToProto(&remoteexecution.ActionResult{}, 10000)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &exampleActionResultMessage, actionResult)
	})
}

// Only provide simple testing coverage for ToByteSlice(). It is built on top
// of exactly the same retry logic as ToProto().
func TestWithErrorHandlerOnCASBuffersToByteSlice(t *testing.T) {
	ctrl := gomock.NewController(t)

	reader1 := mock.NewMockReadCloser(ctrl)
	reader1.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Internal, "Connection closed"))
	reader1.EXPECT().Close().Return(nil)
	b1 := buffer.NewCASBufferFromReader(exampleActionResultDigest, reader1, buffer.UserProvided)
	b2 := buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))

	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
	errorHandler.EXPECT().Done()

	data, err := buffer.WithErrorHandler(b1, errorHandler).ToByteSlice(1000)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)
}

func TestWithErrorHandlerOnCASBuffersToChunkReader(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)

	t.Run("NoRetriesImmediateEOF", func(t *testing.T) {
		// ChunkReader never returns data and errors at the same
		// time. If Reader.Read() returns data and EOF at the
		// same time, the EOF should be saved, so that it may be
		// returned later.
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, []byte("Hello world")), io.EOF
		})
		reader.EXPECT().Close()
		b := buffer.NewCASBufferFromReader(digest, reader, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().Done()

		r := buffer.WithErrorHandler(b, errorHandler).ToChunkReader(
			/* offset = */ 0,
			/* chunk size = */ 12)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), chunk)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("NoRetriesSeparateEOF", func(t *testing.T) {
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, []byte("Hello world")), nil
		})
		reader.EXPECT().Read(gomock.Any()).Return(0, io.EOF)
		reader.EXPECT().Close()
		b := buffer.NewCASBufferFromReader(digest, reader, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().Done()

		r := buffer.WithErrorHandler(b, errorHandler).ToChunkReader(
			/* offset = */ 0,
			/* chunk size = */ 12)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), chunk)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("RetriesFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Hello "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(nil, status.Error(codes.Internal, "No backends available"))
		errorHandler.EXPECT().Done()

		r := buffer.WithErrorHandler(b1, errorHandler).ToChunkReader(
			/* offset = */ 2,
			/* chunk size = */ 10)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("llo "), chunk)
		_, err = r.Read()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "No backends available"), err)
		r.Close()
	})

	t.Run("RetriesSuccess", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Hello "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.UserProvided)

		reader2 := io.NopCloser(bytes.NewBufferString("XXXXXXworld"))
		b2 := buffer.NewCASBufferFromReader(digest, reader2, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		// The start of the second stream is discarded, as the
		// first stream was already written to the output. Data
		// corruption within a part of the stream that is
		// already written is not a problem.
		r := buffer.WithErrorHandler(b1, errorHandler).ToChunkReader(
			/* offset = */ 4,
			/* chunk size = */ 3)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("o "), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("wor"), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("ld"), chunk)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Xyzzy "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		dataIntegrityCallback1 := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback1.EXPECT().Call(false)
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.BackendProvided(dataIntegrityCallback1.Call))

		reader2 := io.NopCloser(bytes.NewBufferString("Hello world"))
		dataIntegrityCallback2 := mock.NewMockDataIntegrityCallback(ctrl)
		b2 := buffer.NewCASBufferFromReader(digest, reader2, buffer.BackendProvided(dataIntegrityCallback2.Call))

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		// Merging the two streams will cause "Xyzzy world" to
		// be returned. This causes a checksum failure. There is
		// no way to recover from this, because we've already
		// written a part of the corrupted data into the output
		// stream.
		r := buffer.WithErrorHandler(b1, errorHandler).ToChunkReader(
			/* offset = */ 0,
			/* chunk size = */ 1000)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("Xyzzy "), chunk)
		_, err = r.Read()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 3c61ab3f7343f99e0d18e0a7dfb3b0ce, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"), err)
		r.Close()
	})
}

func TestWithErrorHandlerOnCASBuffersToReader(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)

	t.Run("RetriesFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Hello "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(nil, status.Error(codes.Internal, "No backends available"))
		errorHandler.EXPECT().Done()

		r := buffer.WithErrorHandler(b1, errorHandler).ToReader()
		_, err := io.ReadAll(r)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "No backends available"), err)
		require.NoError(t, r.Close())
	})

	t.Run("RetriesSuccess", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Hello "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.UserProvided)

		reader2 := io.NopCloser(bytes.NewBufferString("XXXXXXworld"))
		b2 := buffer.NewCASBufferFromReader(digest, reader2, buffer.UserProvided)

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		// The start of the second stream is discarded, as the
		// first stream was already written to the output. Data
		// corruption within a part of the stream that is
		// already written is not a problem.
		r := buffer.WithErrorHandler(b1, errorHandler).ToReader()
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
		require.NoError(t, r.Close())
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		reader1 := mock.NewMockChunkReader(ctrl)
		reader1.EXPECT().Read().Return([]byte("Xyzzy "), nil)
		reader1.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Connection closed"))
		reader1.EXPECT().Close()
		dataIntegrityCallback1 := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback1.EXPECT().Call(false)
		b1 := buffer.NewCASBufferFromChunkReader(digest, reader1, buffer.BackendProvided(dataIntegrityCallback1.Call))

		reader2 := io.NopCloser(bytes.NewBufferString("Hello world"))
		dataIntegrityCallback2 := mock.NewMockDataIntegrityCallback(ctrl)
		b2 := buffer.NewCASBufferFromReader(digest, reader2, buffer.BackendProvided(dataIntegrityCallback2.Call))

		errorHandler := mock.NewMockErrorHandler(ctrl)
		errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
		errorHandler.EXPECT().Done()

		// Merging the two streams will cause "Xyzzy world" to
		// be returned. This causes a checksum failure. There is
		// no way to recover from this, because we've already
		// written a part of the corrupted data into the output
		// stream.
		r := buffer.WithErrorHandler(b1, errorHandler).ToReader()
		data, err := io.ReadAll(r)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 3c61ab3f7343f99e0d18e0a7dfb3b0ce, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"), err)
		require.Equal(t, []byte("Xyzzy "), data)
		require.NoError(t, r.Close())
	})
}

// Only provide simple testing coverage for CloneCopy(). It is built on
// top of ToByteSlice().
func TestWithErrorHandlerOnCASBuffersCloneCopy(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)
	reader1 := mock.NewMockReadCloser(ctrl)
	reader1.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Internal, "Connection closed"))
	reader1.EXPECT().Close().Return(nil)
	b1 := buffer.NewCASBufferFromReader(digest, reader1, buffer.UserProvided)
	b2 := buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))

	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
	errorHandler.EXPECT().Done()

	bc1, bc2 := buffer.WithErrorHandler(b1, errorHandler).CloneCopy(1000)

	data1, err := bc1.ToByteSlice(1000)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data1)

	data2, err := bc2.ToByteSlice(1000)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data2)
}

func TestWithErrorHandlerOnCASBuffersCloneStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)
	reader1 := mock.NewMockReadCloser(ctrl)
	reader1.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Internal, "Connection closed"))
	reader1.EXPECT().Close().Return(nil)
	b1 := buffer.NewCASBufferFromReader(digest, reader1, buffer.UserProvided)
	b2 := buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))

	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Connection closed")).Return(b2, nil)
	errorHandler.EXPECT().Done()

	bc1, bc2 := buffer.WithErrorHandler(b1, errorHandler).CloneStream()
	done := make(chan struct{}, 2)

	go func() {
		data, err := bc1.ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
		done <- struct{}{}
	}()

	go func() {
		data, err := bc2.ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestWithErrorHandlerOnCASBuffersDiscard(t *testing.T) {
	ctrl := gomock.NewController(t)

	chunkReader := mock.NewMockChunkReader(ctrl)
	chunkReader.EXPECT().Close()
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	b1 := buffer.NewCASBufferFromChunkReader(exampleDigest, chunkReader, buffer.BackendProvided(dataIntegrityCallback.Call))

	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().Done()

	buffer.WithErrorHandler(b1, errorHandler).Discard()
}

func TestWithErrorHandlerOnClonedErrorBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)

	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.Internal, "Error message A")).Return(nil, status.Error(codes.Internal, "Error message B"))
	errorHandler.EXPECT().Done()

	b1, b2 := buffer.NewBufferFromError(status.Error(codes.Internal, "Error message A")).CloneCopy(100)
	b2 = buffer.WithErrorHandler(b2, errorHandler)

	// The first consumer will get access to the original error message.
	_, err := b1.ToByteSlice(100)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Error message A"), err)

	// The second consumer will have the error message replaced.
	_, err = b2.ToByteSlice(100)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Error message B"), err)
}
