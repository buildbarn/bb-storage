package buffer_test

import (
	"io"
	"io/ioutil"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewProtoBufferFromByteSliceGetSizeBytes(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Success", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		b := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			exampleActionResultBytes,
			buffer.BackendProvided(dataIntegrityCallback.Call))
		n, err := b.GetSizeBytes()
		require.NoError(t, err)
		require.Equal(t, int64(len(exampleActionResultBytes)), n)
		b.Discard()
	})

	t.Run("DataCorruption", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		b := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			[]byte("Hello world"),
			buffer.BackendProvided(dataIntegrityCallback.Call))
		_, err := b.GetSizeBytes()
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to unmarshal message: proto:"), err)
		b.Discard()
	})
}

func TestNewProtoBufferFromByteSliceReadAt(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Success", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		var p [5]byte
		n, err := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			exampleActionResultBytes,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 0)
		require.Equal(t, 5, n)
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes[:5], p[:])
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		var p [5]byte
		n, err := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			exampleActionResultBytes,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], -123)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -123"), err)
	})

	t.Run("ReadBeyondEOF", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		var p [5]byte
		n, err := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			exampleActionResultBytes,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], int64(len(exampleActionResultBytes)+1))
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("ShortRead", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		var p [5]byte
		n, err := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			exampleActionResultBytes,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], int64(len(exampleActionResultBytes)-3))
		require.Equal(t, 3, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, exampleActionResultBytes[len(exampleActionResultBytes)-3:], p[:3])
	})

	t.Run("DataCorruptionUserProvided", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			[]byte("Hello world"),
			buffer.UserProvided).ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: proto:"), err)
	})

	t.Run("DataCorruptionIrreparable", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			[]byte("Hello world"),
			buffer.BackendProvided(buffer.Irreparable(digest.MustNewDigest("hello", "f988a36ed06e17f6c4a258ec8e03fe88", 123)))).ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to unmarshal message: proto:"), err)
	})

	t.Run("DataCorruptionReparable", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(false)

		var p [5]byte
		n, err := buffer.NewProtoBufferFromByteSlice(
			&remoteexecution.ActionResult{},
			[]byte("Hello world"),
			buffer.BackendProvided(dataIntegrityCallback.Call)).ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to unmarshal message: proto:"), err)
	})
}

// For the remainder of the tests, assume that
// TestNewProtoBufferFromByteSliceReadAt(), TestErrorBuffer*() and
// TestValidatedActionResultBuffer*() test all of the error behavior
// sufficiently. Only test the successful code paths.

func TestNewProtoBufferFromByteSliceToProto(t *testing.T) {
	ctrl := gomock.NewController(t)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)

	actionResult, err := buffer.NewProtoBufferFromByteSlice(
		&remoteexecution.ActionResult{},
		exampleActionResultBytes,
		buffer.BackendProvided(dataIntegrityCallback.Call)).ToProto(&remoteexecution.ActionResult{}, 1000)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &exampleActionResultMessage, actionResult)
}

func TestNewProtoBufferFromByteSliceToByteSlice(t *testing.T) {
	ctrl := gomock.NewController(t)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)

	data, err := buffer.NewProtoBufferFromByteSlice(
		&remoteexecution.ActionResult{},
		exampleActionResultBytes,
		buffer.BackendProvided(dataIntegrityCallback.Call)).ToByteSlice(10000)
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data)
}

func TestNewProtoBufferFromByteSliceToChunkReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)

	r := buffer.NewProtoBufferFromByteSlice(
		&remoteexecution.ActionResult{},
		exampleActionResultBytes,
		buffer.BackendProvided(dataIntegrityCallback.Call)).ToChunkReader(
		/* offset = */ 0,
		/* chunk size = */ 10000)

	data, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data)

	_, err = r.Read()
	require.Equal(t, io.EOF, err)

	r.Close()
}

func TestNewProtoBufferFromByteSliceToReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)

	r := buffer.NewProtoBufferFromByteSlice(
		&remoteexecution.ActionResult{},
		exampleActionResultBytes,
		buffer.BackendProvided(dataIntegrityCallback.Call)).ToReader()

	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data)

	require.NoError(t, r.Close())
}

func TestNewProtoBufferFromByteSliceCloneCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)

	b1, b2 := buffer.NewProtoBufferFromByteSlice(
		&remoteexecution.ActionResult{},
		exampleActionResultBytes,
		buffer.BackendProvided(dataIntegrityCallback.Call)).CloneCopy(len(exampleActionResultBytes))

	data1, err := b1.ToByteSlice(len(exampleActionResultBytes))
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data1)

	data2, err := b2.ToByteSlice(len(exampleActionResultBytes))
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data2)
}

func TestNewProtoBufferFromByteSliceCloneStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)

	b1, b2 := buffer.NewProtoBufferFromByteSlice(
		&remoteexecution.ActionResult{},
		exampleActionResultBytes,
		buffer.BackendProvided(dataIntegrityCallback.Call)).CloneStream()
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

func TestNewProtoBufferFromByteSliceDiscard(t *testing.T) {
	ctrl := gomock.NewController(t)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)

	buffer.NewProtoBufferFromByteSlice(
		&remoteexecution.ActionResult{},
		exampleActionResultBytes,
		buffer.BackendProvided(dataIntegrityCallback.Call)).Discard()
}
