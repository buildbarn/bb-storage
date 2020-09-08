package blobstore_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRedisBlobAccessContextCanceled(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	redisClient := mock.NewMockRedisClient(ctrl)
	blobAccess := blobstore.NewRedisBlobAccess(redisClient, blobstore.CASReadBufferFactory, digest.KeyWithoutInstance, 0, 0)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	blobDigest := digest.MustNewDigest("example", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)

	// Calls to Get(), Put() and FindMissing() should not yield
	// calls into the Redis client if the context associated with
	// the call is canceled.
	//
	// The go-redis client library is not aware of context handles.
	// This means that if these checks were not in place, a larger
	// piece of code that calls into Redis multiple times would not
	// have any cancelation points.
	_, err := blobAccess.Get(canceledCtx, blobDigest).ToByteSlice(100)
	require.Equal(t, err, status.Error(codes.Canceled, "context canceled"))

	err = blobAccess.Put(canceledCtx, blobDigest, buffer.NewValidatedBufferFromByteSlice(nil))
	require.Equal(t, err, status.Error(codes.Canceled, "context canceled"))

	_, err = blobAccess.FindMissing(canceledCtx, digest.EmptySet)
	require.Equal(t, err, status.Error(codes.Canceled, "context canceled"))
}
