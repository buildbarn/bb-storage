package buffer_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorHandlerProtoErrorRewriting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Simple case of using an ErrorHandler to rewrite the error code.
	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.NotFound, "Blob not found")).
		Return(nil, status.Error(codes.FailedPrecondition, "Blob not found"))
	errorHandler.EXPECT().Done()

	_, err := buffer.WithErrorHandler(
		buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")),
		errorHandler).ToByteSlice(123)
	require.Equal(t, status.Error(codes.FailedPrecondition, "Blob not found"), err)
}

func TestErrorHandlerProtoRetryingSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Retrying to load the same blob from storage multiple times.
	// In the end, it should succeed.
	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.NotFound, "Blob not found")).
		Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")), nil).
		Times(4)
	errorHandler.EXPECT().OnError(status.Error(codes.NotFound, "Blob not found")).
		Return(buffer.NewProtoBufferFromProto(&remoteexecution.ActionResult{}, buffer.UserProvided), nil)
	errorHandler.EXPECT().Done()

	actionResult, err := buffer.WithErrorHandler(
		buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")),
		errorHandler).ToProto(&remoteexecution.ActionResult{}, 100)
	require.NoError(t, err)
	require.Equal(t, &remoteexecution.ActionResult{}, actionResult)
}

func TestErrorHandlerProtoRetryingFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Retrying to load the same blob from storage multiple times.
	// In the end, it still fails.
	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().OnError(status.Error(codes.NotFound, "Blob not found")).
		Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")), nil).
		Times(4)
	errorHandler.EXPECT().OnError(status.Error(codes.NotFound, "Blob not found")).
		Return(nil, status.Error(codes.NotFound, "Maximum number of retries reached"))
	errorHandler.EXPECT().Done()

	var p [10]byte
	n, err := buffer.WithErrorHandler(
		buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")),
		errorHandler).ReadAt(p[:], 123)
	require.Equal(t, 0, n)
	require.Equal(t, status.Error(codes.NotFound, "Maximum number of retries reached"), err)
}

func TestErrorHandlerValidatedByteSlice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// No errors can occur on byte slices that are already present
	// in memory.
	errorHandler := mock.NewMockErrorHandler(ctrl)
	errorHandler.EXPECT().Done()

	data, err := buffer.WithErrorHandler(
		buffer.NewValidatedBufferFromByteSlice([]byte("Hello")),
		errorHandler).ToByteSlice(123)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)
}
