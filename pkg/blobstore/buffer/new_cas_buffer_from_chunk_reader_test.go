package buffer_test

import (
	"bytes"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewCASBufferFromChunkReaderGetSizeBytes(t *testing.T) {
	ctrl := gomock.NewController(t)

	helloDigest := digest.MustNewDigest("foo", "8b1a9953c4611296a827abf8c47804d7", 5)
	chunkReader := mock.NewMockChunkReader(ctrl)
	chunkReader.EXPECT().Close()

	b := buffer.NewCASBufferFromChunkReader(helloDigest, chunkReader, buffer.BackendProvided(buffer.Irreparable(helloDigest)))
	n, err := b.GetSizeBytes()
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	b.Discard()
}

func TestNewCASBufferFromChunkReaderIntoWriter(t *testing.T) {
	ctrl := gomock.NewController(t)

	helloDigest := digest.MustNewDigest("foo", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		writer := bytes.NewBuffer(nil)
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).IntoWriter(writer)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), writer.Bytes())
	})

	t.Run("IOError", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Storage backend on fire"))
		chunkReader.EXPECT().Close()
		writer := mock.NewMockWriter(ctrl)
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).IntoWriter(writer)
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		writer := mock.NewMockWriter(ctrl)
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).IntoWriter(writer)
		require.Equal(t, status.Error(codes.Internal, "Buffer is 0 bytes in size, while 5 bytes were expected"), err)
	})
}

func TestNewCASBufferFromChunkReaderReadAt(t *testing.T) {
	ctrl := gomock.NewController(t)

	helloDigest := digest.MustNewDigest("foo", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		var p [3]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 1)
		require.Equal(t, 3, n)
		require.NoError(t, err)
		require.Equal(t, []byte("ell"), p[:])
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		var p [5]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], -123)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -123"), err)
	})

	t.Run("ReadBeyondEOF", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("He"), nil)
		chunkReader.EXPECT().Read().Return([]byte("ll"), nil)
		chunkReader.EXPECT().Read().Return([]byte("o"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		var p [5]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 6)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("ShortRead", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		var p [5]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 2)
		require.Equal(t, 3, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("llo"), p[:3])
	})

	t.Run("SizeTooSmall", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Foo"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		var p [2]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 1)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Buffer is 3 bytes in size, while 5 bytes were expected"), err)
	})

	t.Run("SizeTooLarge", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Foo"), nil)
		chunkReader.EXPECT().Read().Return([]byte("Bar"), nil)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		var p [2]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 1)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Buffer is at least 6 bytes in size, while 5 bytes were expected"), err)
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Xyzzy"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		var p [2]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 1)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Buffer has checksum 56f2d4d0b97e43f94505299dc45942a1, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)
	})

	t.Run("IOFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Storage backend on fire"))
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		var p [2]byte
		n, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 1)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
	})
}

func TestNewCASBufferFromChunkReaderToProto(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("SmallerThanMaximum", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(exampleActionResultBytes, nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		actionResult, err := buffer.NewCASBufferFromChunkReader(
			exampleActionResultDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes)+1)
		require.NoError(t, err)
		require.True(t, proto.Equal(&exampleActionResultMessage, actionResult))
	})

	t.Run("Exact", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(exampleActionResultBytes, nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		actionResult, err := buffer.NewCASBufferFromChunkReader(
			exampleActionResultDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.NoError(t, err)
		require.True(t, proto.Equal(&exampleActionResultMessage, actionResult))
	})

	t.Run("TooBig", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		_, err := buffer.NewCASBufferFromChunkReader(
			exampleActionResultDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes)-1)
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 134 bytes in size, while a maximum of 133 bytes is permitted"), err)
	})

	t.Run("DataCorruption", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Foo"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		_, err := buffer.NewCASBufferFromChunkReader(
			exampleActionResultDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.Equal(t, status.Error(codes.Internal, "Buffer is 3 bytes in size, while 134 bytes were expected"), err)
	})

	t.Run("InvalidProtobuf", func(t *testing.T) {
		// Failing to unmarshal Protobufs stored in the CAS
		// should not be treated as a data integrity error if
		// the hash of the object matches. That's an error on
		// the consumption side.
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		helloDigest := digest.MustNewDigest("foo", "8b1a9953c4611296a827abf8c47804d7", 5)
		_, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.Equal(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: proto: can't skip unknown wire type 4"), err)
	})

	t.Run("IOFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Storage backend on fire"))
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		_, err := buffer.NewCASBufferFromChunkReader(
			exampleActionResultDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
	})
}

func TestNewCASBufferFromChunkReaderToByteSlice(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Only test the successful case, as other aspects are already
	// covered by TestNewCASBufferFromChunkReaderToProto.
	t.Run("Success", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("H"), nil)
		chunkReader.EXPECT().Read().Return([]byte("ello"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		helloDigest := digest.MustNewDigest("foo", "8b1a9953c4611296a827abf8c47804d7", 5)
		data, err := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		// Require that byte slices obtained from chunk readers
		// don't waste any additional memory.
		require.Equal(t, 5, cap(data))
	})

	t.Run("Empty", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		emptyDigest := digest.MustNewDigest("empty", "d41d8cd98f00b204e9800998ecf8427e", 0)
		data, err := buffer.NewCASBufferFromChunkReader(
			emptyDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToByteSlice(10)
		require.NoError(t, err)
		require.Empty(t, data)
	})
}

func TestNewCASBufferFromChunkReaderToChunkReader(t *testing.T) {
	ctrl := gomock.NewController(t)

	helloDigest := digest.MustNewDigest("foo", "3e25960a79dbc69b674cd4ec67a72c62", 11)

	t.Run("Success", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("H"), nil)
		chunkReader.EXPECT().Read().Return([]byte(""), nil)
		chunkReader.EXPECT().Read().Return([]byte("ello"), nil)
		chunkReader.EXPECT().Read().Return([]byte(" "), nil)
		chunkReader.EXPECT().Read().Return([]byte(""), nil)
		chunkReader.EXPECT().Read().Return([]byte("world"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		// The ChunkReader returned by ToChunkReader() should
		// omit empty chunks and split up chunks that are too
		// large.
		r := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToChunkReader(
			/* offset = */ 3,
			/* chunk size = */ 2)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("lo"), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte(" "), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("wo"), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("rl"), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("d"), chunk)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("AtTheEnd", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello world"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		// Reading at the very end is permitted, but should
		// return an end-of-file immediately.
		r := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToChunkReader(
			/* offset = */ 11,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		r := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToChunkReader(
			/* offset = */ -1,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -1"), err)
		r.Close()
	})

	t.Run("TooFar", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		r := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToChunkReader(
			/* offset = */ 12,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 11 bytes in size, while a read at offset 12 was requested"), err)
		r.Close()
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello "), nil)
		chunkReader.EXPECT().Read().Return([]byte("worlf"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		// In case of checksum failures, it should not be
		// possible to extract the final piece of data.
		r := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToChunkReader(
			/* offset = */ 0,
			/* chunk size = */ 10)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello "), chunk)
		_, err = r.Read()
		require.Equal(t, status.Error(codes.Internal, "Buffer has checksum d46893336c594d884bb1b9b4f5299f4a, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"), err)
		_, err = r.Read()
		require.Equal(t, status.Error(codes.Internal, "Buffer has checksum d46893336c594d884bb1b9b4f5299f4a, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"), err)
		r.Close()
	})
}

func TestNewCASBufferFromChunkReaderToReader(t *testing.T) {
	ctrl := gomock.NewController(t)

	helloDigest := digest.MustNewDigest("foo", "3e25960a79dbc69b674cd4ec67a72c62", 11)

	t.Run("Success", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("H"), nil)
		chunkReader.EXPECT().Read().Return([]byte(""), nil)
		chunkReader.EXPECT().Read().Return([]byte("ello"), nil)
		chunkReader.EXPECT().Read().Return([]byte(" "), nil)
		chunkReader.EXPECT().Read().Return([]byte(""), nil)
		chunkReader.EXPECT().Read().Return([]byte("world"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		r := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToReader()
		var p [3]byte
		n, err := r.Read(p[:])
		require.Equal(t, 3, n)
		require.NoError(t, err)
		require.Equal(t, []byte("Hel"), p[:])
		n, err = r.Read(p[:])
		require.Equal(t, 3, n)
		require.NoError(t, err)
		require.Equal(t, []byte("lo "), p[:])
		n, err = r.Read(p[:])
		require.Equal(t, 3, n)
		require.NoError(t, err)
		require.Equal(t, []byte("wor"), p[:])
		n, err = r.Read(p[:])
		require.Equal(t, 2, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("ld"), p[:2])
		n, err = r.Read(p[:])
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
		require.Nil(t, r.Close())
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello "), nil)
		chunkReader.EXPECT().Read().Return([]byte("worlf"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		// In case of checksum failures, it should not be
		// possible to extract the final piece of data.
		r := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToReader()
		var p [20]byte
		n, err := r.Read(p[:])
		require.Equal(t, 6, n)
		require.Equal(t, status.Error(codes.Internal, "Buffer has checksum d46893336c594d884bb1b9b4f5299f4a, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"), err)
		require.Equal(t, []byte("Hello "), p[:6])
		n, err = r.Read(p[:])
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Buffer has checksum d46893336c594d884bb1b9b4f5299f4a, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"), err)
		require.Nil(t, r.Close())
	})
}

func TestNewCASBufferFromChunkReaderCloneCopy(t *testing.T) {
	ctrl := gomock.NewController(t)

	helloDigest := digest.MustNewDigest("foo", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		b1, b2 := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).CloneCopy(10)

		data1, err := b1.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data1)

		data2, err := b2.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data2)
	})

	t.Run("IOError", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Storage backend on fire"))
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		b1, b2 := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).CloneCopy(10)

		_, err := b1.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)

		_, err = b2.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		b1, b2 := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).CloneCopy(10)

		_, err := b1.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Buffer is 0 bytes in size, while 5 bytes were expected"), err)

		_, err = b2.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Buffer is 0 bytes in size, while 5 bytes were expected"), err)
	})

	t.Run("TooBig", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		b1, b2 := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).CloneCopy(4)

		_, err := b1.ToByteSlice(10)
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 5 bytes in size, while a maximum of 4 bytes is permitted"), err)

		_, err = b2.ToByteSlice(10)
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 5 bytes in size, while a maximum of 4 bytes is permitted"), err)
	})
}

func TestNewCASBufferFromChunkReaderCloneStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	helloDigest := digest.MustNewDigest("foo", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return([]byte("Hello"), nil)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		b1, b2 := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).CloneStream()
		done := make(chan struct{}, 2)

		go func() {
			data, err := b1.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			done <- struct{}{}
		}()

		go func() {
			data, err := b2.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			done <- struct{}{}
		}()

		<-done
		<-done
	})

	t.Run("IOError", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, status.Error(codes.Internal, "Storage backend on fire"))
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

		b1, b2 := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).CloneStream()
		done := make(chan struct{}, 2)

		go func() {
			_, err := b1.ToByteSlice(10)
			require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
			done <- struct{}{}
		}()

		go func() {
			_, err := b2.ToByteSlice(10)
			require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
			done <- struct{}{}
		}()

		<-done
		<-done
	})

	t.Run("ChecksumFailure", func(t *testing.T) {
		chunkReader := mock.NewMockChunkReader(ctrl)
		chunkReader.EXPECT().Read().Return(nil, io.EOF)
		chunkReader.EXPECT().Close()
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		b1, b2 := buffer.NewCASBufferFromChunkReader(
			helloDigest,
			chunkReader,
			buffer.BackendProvided(dataIntegrityCallback.Call)).CloneStream()
		done := make(chan struct{}, 2)

		go func() {
			_, err := b1.ToByteSlice(10)
			require.Equal(t, status.Error(codes.Internal, "Buffer is 0 bytes in size, while 5 bytes were expected"), err)
			done <- struct{}{}
		}()

		go func() {
			_, err := b2.ToByteSlice(10)
			require.Equal(t, status.Error(codes.Internal, "Buffer is 0 bytes in size, while 5 bytes were expected"), err)
			done <- struct{}{}
		}()

		<-done
		<-done
	})
}

func TestNewCASBufferFromChunkReaderDiscard(t *testing.T) {
	ctrl := gomock.NewController(t)

	chunkReader := mock.NewMockChunkReader(ctrl)
	chunkReader.EXPECT().Close()
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)

	buffer.NewCASBufferFromChunkReader(exampleDigest, chunkReader, buffer.BackendProvided(dataIntegrityCallback.Call)).Discard()
}
