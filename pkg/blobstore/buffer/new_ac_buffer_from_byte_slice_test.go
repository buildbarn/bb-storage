package buffer_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewACBufferFromByteSliceGetSizeBytes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Success", func(t *testing.T) {
		b := buffer.NewACBufferFromByteSlice(exampleActionResultBytes, buffer.Irreparable)
		n, err := b.GetSizeBytes()
		require.NoError(t, err)
		require.Equal(t, int64(len(exampleActionResultBytes)), n)
		b.Discard()
	})

	t.Run("DataCorruption", func(t *testing.T) {
		b := buffer.NewACBufferFromByteSlice([]byte("Hello world"), buffer.Irreparable)
		_, err := b.GetSizeBytes()
		require.Equal(t, status.Error(codes.Internal, "Failed to unmarshal message: proto: can't skip unknown wire type 4"), err)
		b.Discard()
	})
}

func TestNewACBufferFromByteSliceReadAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Success", func(t *testing.T) {
		repairFunc := mock.NewMockRepairFunc(ctrl)

		var p [5]byte
		n, err := buffer.NewACBufferFromByteSlice(
			exampleActionResultBytes,
			buffer.Reparable(exampleDigest, repairFunc.Call)).ReadAt(p[:], 0)
		require.Equal(t, 5, n)
		require.NoError(t, err)
		require.Equal(t, exampleActionResultBytes[:5], p[:])
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		repairFunc := mock.NewMockRepairFunc(ctrl)

		var p [5]byte
		n, err := buffer.NewACBufferFromByteSlice(
			exampleActionResultBytes,
			buffer.Reparable(exampleDigest, repairFunc.Call)).ReadAt(p[:], -123)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -123"), err)
	})

	t.Run("ReadBeyondEOF", func(t *testing.T) {
		repairFunc := mock.NewMockRepairFunc(ctrl)

		var p [5]byte
		n, err := buffer.NewACBufferFromByteSlice(
			exampleActionResultBytes,
			buffer.Reparable(exampleDigest, repairFunc.Call)).ReadAt(p[:], int64(len(exampleActionResultBytes)+1))
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("ShortRead", func(t *testing.T) {
		repairFunc := mock.NewMockRepairFunc(ctrl)

		var p [5]byte
		n, err := buffer.NewACBufferFromByteSlice(
			exampleActionResultBytes,
			buffer.Reparable(exampleDigest, repairFunc.Call)).ReadAt(p[:], int64(len(exampleActionResultBytes)-3))
		require.Equal(t, 3, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, exampleActionResultBytes[len(exampleActionResultBytes)-3:], p[:3])
	})

	t.Run("DataCorruptionUserProvided", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewACBufferFromByteSlice(
			[]byte("Hello world"),
			buffer.UserProvided).ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: proto: can't skip unknown wire type 4"), err)
	})

	t.Run("DataCorruptionIrreparable", func(t *testing.T) {
		var p [5]byte
		n, err := buffer.NewACBufferFromByteSlice(
			[]byte("Hello world"),
			buffer.Irreparable).ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Failed to unmarshal message: proto: can't skip unknown wire type 4"), err)
	})

	t.Run("DataCorruptionReparable", func(t *testing.T) {
		repairFunc := mock.NewMockRepairFunc(ctrl)
		repairFunc.EXPECT().Call()

		var p [5]byte
		n, err := buffer.NewACBufferFromByteSlice(
			[]byte("Hello world"),
			buffer.Reparable(exampleDigest, repairFunc.Call)).ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Failed to unmarshal message: proto: can't skip unknown wire type 4"), err)
	})
}

// For the remainder of the tests, assume that
// TestNewACBufferFromByteSliceReadAt(), TestACErrorBuffer*() and
// TestValidatedActionResultBuffer*() test all of the error behavior
// sufficiently. Only test the successful code paths.

func TestNewACBufferFromByteSliceToActionResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repairFunc := mock.NewMockRepairFunc(ctrl)

	actionResult, err := buffer.NewACBufferFromByteSlice(
		exampleActionResultBytes,
		buffer.Reparable(exampleDigest, repairFunc.Call)).ToActionResult(1000)
	require.NoError(t, err)
	require.True(t, proto.Equal(&exampleActionResultMessage, actionResult))
}

func TestNewACBufferFromByteSliceToByteSlice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repairFunc := mock.NewMockRepairFunc(ctrl)

	data, err := buffer.NewACBufferFromByteSlice(
		exampleActionResultBytes,
		buffer.Reparable(exampleDigest, repairFunc.Call)).ToByteSlice(10000)
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data)
}

func TestNewACBufferFromByteSliceToChunkReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repairFunc := mock.NewMockRepairFunc(ctrl)

	r := buffer.NewACBufferFromByteSlice(
		exampleActionResultBytes,
		buffer.Reparable(exampleDigest, repairFunc.Call)).ToChunkReader(
		/* offset = */ 0,
		/* chunk size = */ 10000)

	data, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data)

	_, err = r.Read()
	require.Equal(t, io.EOF, err)

	r.Close()
}

func TestNewACBufferFromByteSliceToReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repairFunc := mock.NewMockRepairFunc(ctrl)

	r := buffer.NewACBufferFromByteSlice(
		exampleActionResultBytes,
		buffer.Reparable(exampleDigest, repairFunc.Call)).ToReader()

	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data)

	require.NoError(t, r.Close())
}

func TestNewACBufferFromByteSliceCloneCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repairFunc := mock.NewMockRepairFunc(ctrl)

	b1, b2 := buffer.NewACBufferFromByteSlice(
		exampleActionResultBytes,
		buffer.Reparable(exampleDigest, repairFunc.Call)).CloneCopy(len(exampleActionResultBytes))

	data1, err := b1.ToByteSlice(len(exampleActionResultBytes))
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data1)

	data2, err := b2.ToByteSlice(len(exampleActionResultBytes))
	require.NoError(t, err)
	require.Equal(t, exampleActionResultBytes, data2)
}

func TestNewACBufferFromByteSliceCloneStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repairFunc := mock.NewMockRepairFunc(ctrl)

	b1, b2 := buffer.NewACBufferFromByteSlice(
		exampleActionResultBytes,
		buffer.Reparable(exampleDigest, repairFunc.Call)).CloneStream()
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

func TestNewACBufferFromByteSliceDiscard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repairFunc := mock.NewMockRepairFunc(ctrl)

	buffer.NewACBufferFromByteSlice(
		exampleActionResultBytes,
		buffer.Reparable(exampleDigest, repairFunc.Call)).Discard()
}
