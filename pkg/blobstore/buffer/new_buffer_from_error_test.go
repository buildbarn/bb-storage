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

func TestNewBufferFromErrorIntoWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	out := mock.NewMockWriter(ctrl)
	err := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).IntoWriter(out)
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
}

func TestNewBufferFromErrorReadAt(t *testing.T) {
	var p [5]byte
	n, err := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).ReadAt(p[:], 123)
	require.Equal(t, 0, n)
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
}

func TestNewBufferFromErrorToProto(t *testing.T) {
	_, err := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).ToProto(&remoteexecution.ActionResult{}, 100)
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
}

func TestNewBufferFromErrorToByteSlice(t *testing.T) {
	_, err := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).ToByteSlice(123)
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
}

func TestNewBufferFromErrorToChunkReader(t *testing.T) {
	r := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).ToChunkReader(
		/* offset = */ 12,
		/* chunk size = */ 10)

	_, err := r.Read()
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)

	r.Close()
}

func TestNewBufferFromErrorToReader(t *testing.T) {
	r := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).ToReader()

	var p [10]byte
	n, err := r.Read(p[:])
	require.Equal(t, 0, n)
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)

	require.NoError(t, r.Close())
}

func TestNewBufferFromErrorCloneCopy(t *testing.T) {
	b1, b2 := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).CloneCopy(100)

	_, err := b1.ToByteSlice(123)
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)

	_, err = b2.ToByteSlice(123)
	require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
}

func TestNewBufferFromErrorCloneStream(t *testing.T) {
	b1, b2 := buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).CloneStream()
	done := make(chan struct{}, 2)

	go func() {
		_, err := b1.ToByteSlice(123)
		require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
		done <- struct{}{}
	}()

	go func() {
		_, err := b2.ToByteSlice(123)
		require.Equal(t, status.Error(codes.Internal, "I/O error"), err)
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestNewBufferFromErrorDiscard(t *testing.T) {
	buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")).Discard()
}
