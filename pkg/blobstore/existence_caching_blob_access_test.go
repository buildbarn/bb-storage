package blobstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExistenceCachingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	blobAccess := blobstore.NewExistenceCachingBlobAccess(
		baseBlobAccess,
		digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet()))

	bothDigests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)).
		Add(digest.MustNewDigest("instance", "78ae647dc5544d227130a0682a51e30bc7777fbb6d8a8f17007463a3ecd1d524", 5)).
		Build()
	nonExistingDigests := digest.MustNewDigest("instance", "78ae647dc5544d227130a0682a51e30bc7777fbb6d8a8f17007463a3ecd1d524", 5).ToSingletonSet()

	// As the cache is empty upon initialization, the first request
	// should cause both digests to be queried on the backend.
	clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(2)
	baseBlobAccess.EXPECT().FindMissing(ctx, bothDigests).Return(nonExistingDigests, nil)
	missing, err := blobAccess.FindMissing(ctx, bothDigests)
	require.NoError(t, err)
	require.Equal(t, nonExistingDigests, missing)

	// The existing object should be cached for up to a minute,
	// causing FindMissing() on the backend to be called with the
	// nonexisting one.
	clock.EXPECT().Now().Return(time.Unix(1060, 0)).Times(2)
	baseBlobAccess.EXPECT().FindMissing(ctx, nonExistingDigests).Return(nonExistingDigests, nil)
	missing, err = blobAccess.FindMissing(ctx, bothDigests)
	require.NoError(t, err)
	require.Equal(t, nonExistingDigests, missing)

	// Once the cache entry has expired, both objects should be
	// requested once again.
	clock.EXPECT().Now().Return(time.Unix(1060, 1)).Times(2)
	baseBlobAccess.EXPECT().FindMissing(ctx, bothDigests).Return(nonExistingDigests, nil)
	missing, err = blobAccess.FindMissing(ctx, bothDigests)
	require.NoError(t, err)
	require.Equal(t, nonExistingDigests, missing)
}

func TestExistenceCachingBlobAccessGetNotExists(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	blobAccess := blobstore.NewExistenceCachingBlobAccess(
		baseBlobAccess,
		digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet()))

	digest1 := digest.MustNewDigest("instance", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	digest2 := digest.MustNewDigest("instance", "78ae647dc5544d227130a0682a51e30bc7777fbb6d8a8f17007463a3ecd1d524", 5)
	bothDigests := digest.NewSetBuilder().Add(digest1).Add(digest2).Build()
	oneDigest := digest.NewSetBuilder().Add(digest1).Build()

	// Prime the cache
	clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(2)
	baseBlobAccess.EXPECT().FindMissing(ctx, bothDigests).Return(digest.EmptySet, nil)
	missing, err := blobAccess.FindMissing(ctx, bothDigests)
	require.NoError(t, err)
	require.Equal(t, digest.EmptySet, missing)

	// Query for digest 1, and return NotFound, evicting it
	baseBlobAccess.EXPECT().Get(ctx, digest1).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
	_, err = blobAccess.Get(ctx, digest1).ToByteSlice(100)
	require.Equal(t, status.Error(codes.NotFound, "Blob not found"), err)

	// Not enough time has elapsed for the cache to expire, but
	// digest1 is still removed
	clock.EXPECT().Now().Return(time.Unix(1030, 0)).Times(2)
	baseBlobAccess.EXPECT().FindMissing(ctx, oneDigest).Return(oneDigest, nil)
	missing, err = blobAccess.FindMissing(ctx, bothDigests)
	require.NoError(t, err)
	require.Equal(t, oneDigest, missing)

	// Query for digest 2, and return Unavailable, not evicting it
	baseBlobAccess.EXPECT().Get(ctx, digest2).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server not responding")))
	_, err = blobAccess.Get(ctx, digest2).ToByteSlice(100)
	require.Equal(t, status.Error(codes.Unavailable, "Server not responding"), err)

	// digest2 is still present
	clock.EXPECT().Now().Return(time.Unix(1030, 0)).Times(2)
	baseBlobAccess.EXPECT().FindMissing(ctx, oneDigest).Return(oneDigest, nil)
	missing, err = blobAccess.FindMissing(ctx, bothDigests)
	require.NoError(t, err)
	require.Equal(t, oneDigest, missing)
}
