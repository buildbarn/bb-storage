package local_test

import (
	"errors"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClosableBlockListPushBack(t *testing.T) {
	ctrl := gomock.NewController(t)

	backendBlockList := mock.NewMockBlockList(ctrl)
	closableBlockList := local.NewClosableBlockList(backendBlockList)

	myPushBackError := errors.New("PushBack")
	backendBlockList.EXPECT().PushBack().Return(myPushBackError)
	require.Equal(t, myPushBackError, closableBlockList.PushBack())

	backendBlockList.EXPECT().PushBack().Return(nil)
	require.NoError(t, closableBlockList.PushBack())

	// The backend should not be called from now on.
	closableBlockList.CloseForWriting()

	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Closed for writing"), closableBlockList.PushBack())
}

func TestClosableBlockListSuccessfulPut(t *testing.T) {
	ctrl := gomock.NewController(t)

	backendBlockList := mock.NewMockBlockList(ctrl)
	closableBlockList := local.NewClosableBlockList(backendBlockList)

	backendPutWriter := mock.NewMockBlockListPutWriter(ctrl)
	backendBlockList.EXPECT().Put(2, int64(5)).Return(backendPutWriter.Call)
	putWriter := closableBlockList.Put(2, int64(5))

	backendPutFinalizer := mock.NewMockBlockListPutFinalizer(ctrl)
	backendPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
		func(b buffer.Buffer) local.BlockListPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return backendPutFinalizer.Call
		})
	putFinalizer := putWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

	myPutError := errors.New("Put")
	backendPutFinalizer.EXPECT().Call().Return(int64(123), myPutError)
	offset, err := putFinalizer()
	require.Equal(t, int64(123), offset)
	require.Equal(t, myPutError, err)
}

func TestClosableBlockListPutWhenClosedForWriting(t *testing.T) {
	// Writing to a block list consists of calling Put(), which returns a
	// putWriter to be called, which in turn returns a putFinalizer to be
	// called. Make sure error is returned if closed for writing in the last
	// minute.
	ctrl := gomock.NewController(t)

	backendBlockList := mock.NewMockBlockList(ctrl)
	closableBlockList := local.NewClosableBlockList(backendBlockList)

	backendPutWriter := mock.NewMockBlockListPutWriter(ctrl)
	backendBlockList.EXPECT().Put(2, int64(5)).Return(backendPutWriter.Call)
	putWriter := closableBlockList.Put(2, int64(5))

	backendPutFinalizer := mock.NewMockBlockListPutFinalizer(ctrl)
	backendPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
		func(b buffer.Buffer) local.BlockListPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return backendPutFinalizer.Call
		})
	putFinalizer := putWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

	// Close for writing while the putFinalizer is called, in the last moment.
	backendPutFinalizer.EXPECT().Call().DoAndReturn(
		func() (int64, error) {
			closableBlockList.CloseForWriting()
			return int64(123), nil
		})
	_, err := putFinalizer()
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Closed for writing"), err)
}
