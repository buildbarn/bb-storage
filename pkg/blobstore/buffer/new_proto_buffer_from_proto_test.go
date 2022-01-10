package buffer_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewProtoBufferFromProtoGetSizeBytes(t *testing.T) {
	b := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided)
	n, err := b.GetSizeBytes()
	require.NoError(t, err)
	require.Equal(t, int64(len(exampleActionResultBytes)), n)
	b.Discard()
}

func TestNewProtoBufferFromProtoReadAt(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ReadAt(p[:], 0)
		require.Equal(t, 5, n)
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes[:5], p[:])
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ReadAt(p[:], -123)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -123"), err)
	})

	t.Run("ReadBeyondEOF", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ReadAt(p[:], int64(len(exampleActionResultBytes)+1))
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("ShortRead", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ReadAt(p[:], int64(len(exampleActionResultBytes)-3))
		require.Equal(t, 3, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, exampleActionResultBytes[len(exampleActionResultBytes)-3:], p[:3])
	})
}

func TestNewProtoBufferFromProtoIntoWriter(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Success", func(t *testing.T) {
		out := bytes.NewBuffer(nil)
		err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).IntoWriter(out)
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, out.Bytes())
	})

	t.Run("IOFailure", func(t *testing.T) {
		out := mock.NewMockWriter(ctrl)
		out.EXPECT().Write(gomock.Any()).Return(0, status.Error(codes.Internal, "Storage backend unavailable"))
		err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).IntoWriter(out)
		require.Equal(t, status.Error(codes.Internal, "Storage backend unavailable"), err)
	})
}

func TestNewProtoBufferFromProtoToProto(t *testing.T) {
	t.Run("SmallerThanMaximum", func(t *testing.T) {
		actionResult, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes)+1)
		require.NoError(t, err)
		require.Equal(t, &exampleActionResultMessage, actionResult)
	})

	t.Run("Exact", func(t *testing.T) {
		actionResult, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.NoError(t, err)
		require.Equal(t, &exampleActionResultMessage, actionResult)
	})

	t.Run("TooBig", func(t *testing.T) {
		_, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes)-1)
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 134 bytes in size, while a maximum of 133 bytes is permitted"), err)
	})
}

func TestNewProtoBufferFromProtoToByteSlice(t *testing.T) {
	t.Run("SmallerThanMaximum", func(t *testing.T) {
		data, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToByteSlice(len(exampleActionResultBytes) + 1)
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, data)
	})

	t.Run("Exact", func(t *testing.T) {
		data, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToByteSlice(len(exampleActionResultBytes))
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, data)
	})

	t.Run("TooBig", func(t *testing.T) {
		_, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToByteSlice(len(exampleActionResultBytes) - 1)
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 134 bytes in size, while a maximum of 133 bytes is permitted"), err)
	})
}

func TestNewProtoBufferFromProtoToChunkReader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		r := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToChunkReader(
			/* offset = */ 12,
			/* chunk size = */ 10)

		off := 12
		for ; off < len(exampleActionResultBytes)-10; off += 10 {
			data, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, exampleActionResultBytes[off:off+10], data)
		}

		data, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes[off:], data)

		_, err = r.Read()
		require.Equal(t, io.EOF, err)

		r.Close()
	})

	t.Run("AtTheEnd", func(t *testing.T) {
		// Reading at the very end is permitted, but should
		// return an end-of-file immediately.
		r := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToChunkReader(
			/* offset = */ int64(len(exampleActionResultBytes)),
			/* chunk size = */ 10)
		_, err := r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		r := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToChunkReader(
			/* offset = */ -123,
			/* chunk size = */ 1024)

		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -123"), err)

		r.Close()
	})

	t.Run("TooFar", func(t *testing.T) {
		r := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToChunkReader(
			/* offset = */ int64(len(exampleActionResultBytes)+1),
			/* chunk size = */ 100)

		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 134 bytes in size, while a read at offset 135 was requested"), err)

		r.Close()
	})
}

func TestNewProtoBufferFromProtoToReader(t *testing.T) {
	r := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).ToReader()

	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data)

	require.NoError(t, r.Close())
}

func TestNewProtoBufferFromProtoCloneCopy(t *testing.T) {
	b1, b2 := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).CloneCopy(len(exampleActionResultBytes))

	data1, err := b1.ToByteSlice(len(exampleActionResultBytes))
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data1)

	data2, err := b2.ToByteSlice(len(exampleActionResultBytes))
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data2)
}

func TestNewProtoBufferFromProtoWithTask(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		data, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).
			WithTask(func() error { return nil }).
			ToByteSlice(len(exampleActionResultBytes))
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, data)
	})

	t.Run("Failure", func(t *testing.T) {
		_, err := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).
			WithTask(func() error { return status.Error(codes.Internal, "I/O error") }).
			ToByteSlice(len(exampleActionResultBytes))
		require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
	})
}

func TestNewProtoBufferFromProtoCloneStream(t *testing.T) {
	b1, b2 := buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).CloneStream()
	done := make(chan struct{}, 2)

	go func() {
		data, err := b1.ToByteSlice(len(exampleActionResultBytes))
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, data)
		done <- struct{}{}
	}()

	go func() {
		data, err := b2.ToByteSlice(len(exampleActionResultBytes))
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes, data)
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestNewProtoBufferFromProtoDiscard(t *testing.T) {
	buffer.NewProtoBufferFromProto(&exampleActionResultMessage, buffer.UserProvided).Discard()
}
