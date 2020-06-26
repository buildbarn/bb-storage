package buffer_test

import (
	"io"
	"io/ioutil"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewValidatedBufferFromByteSliceReadAt(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ReadAt(p[:], 0)
		require.Equal(t, 5, n)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), p[:])
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ReadAt(p[:], -123)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -123"), err)
	})

	t.Run("ReadBeyondEOF", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ReadAt(p[:], 6)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("ShortRead", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ReadAt(p[:], 2)
		require.Equal(t, 3, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("llo"), p[:3])
	})
}

func TestNewValidatedBufferFromByteSliceToProto(t *testing.T) {
	t.Run("SmallerThanMaximum", func(t *testing.T) {
		actionResult, err := buffer.NewValidatedBufferFromByteSlice(exampleActionResultBytes).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes)+1)
		require.NoError(t, err)
		require.True(t, proto.Equal(&exampleActionResultMessage, actionResult))
	})

	t.Run("Exact", func(t *testing.T) {
		actionResult, err := buffer.NewValidatedBufferFromByteSlice(exampleActionResultBytes).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.NoError(t, err)
		require.True(t, proto.Equal(&exampleActionResultMessage, actionResult))
	})

	t.Run("TooBig", func(t *testing.T) {
		_, err := buffer.NewValidatedBufferFromByteSlice(exampleActionResultBytes).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes)-1)
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 134 bytes in size, while a maximum of 133 bytes is permitted"), err)
	})

	t.Run("Failure", func(t *testing.T) {
		_, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")).
			ToProto(&remoteexecution.ActionResult{}, 100)
		require.Equal(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: proto: can't skip unknown wire type 4"), err)
	})
}

func TestNewValidatedBufferFromByteSliceToByteSlice(t *testing.T) {
	t.Run("SmallerThanMaximum", func(t *testing.T) {
		data, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToByteSlice(6)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("Exact", func(t *testing.T) {
		data, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToByteSlice(5)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("TooBig", func(t *testing.T) {
		_, err := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToByteSlice(4)
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 5 bytes in size, while a maximum of 4 bytes is permitted"), err)
	})
}

func TestNewValidatedBufferFromByteSliceToChunkReader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		r := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToChunkReader(
			/* offset = */ 1,
			/* chunk size = */ 2)

		data, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("el"), data)

		data, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("lo"), data)

		_, err = r.Read()
		require.Equal(t, io.EOF, err)

		_, err = r.Read()
		require.Equal(t, io.EOF, err)

		r.Close()
	})

	t.Run("AtTheEnd", func(t *testing.T) {
		// Reading at the very end is permitted, but should
		// return an end-of-file immediately.
		r := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToChunkReader(
			/* offset = */ 5,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		r := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToChunkReader(
			/* offset = */ -123,
			/* chunk size = */ 1024)

		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -123"), err)

		r.Close()
	})

	t.Run("TooFar", func(t *testing.T) {
		r := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToChunkReader(
			/* offset = */ 6,
			/* chunk size = */ 1024)

		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 5 bytes in size, while a read at offset 6 was requested"), err)

		r.Close()
	})
}

func TestNewValidatedBufferFromByteSliceToReader(t *testing.T) {
	r := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).ToReader()

	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)

	require.NoError(t, r.Close())
}

func TestNewValidatedBufferFromByteSliceCloneCopy(t *testing.T) {
	b1, b2 := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).CloneCopy(10)

	data1, err := b1.ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data1)

	data2, err := b2.ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data2)
}

func TestNewValidatedBufferFromByteSliceCloneStream(t *testing.T) {
	b1, b2 := buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).CloneStream()
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
}

func TestNewValidatedBufferFromByteSliceDiscard(t *testing.T) {
	buffer.NewValidatedBufferFromByteSlice([]byte("Hello")).Discard()
}
