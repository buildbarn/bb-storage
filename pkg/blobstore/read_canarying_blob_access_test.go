package blobstore_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadCanaryingBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	sourceBackend := mock.NewMockBlobAccess(ctrl)
	sourceBackend.EXPECT().FindMissing(ctx, digest.EmptySet).Return(digest.EmptySet, nil).AnyTimes()
	replicaBackend := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	replicaErrorLogger := mock.NewMockErrorLogger(ctrl)
	blobAccess := blobstore.NewReadCanaryingBlobAccess(
		sourceBackend,
		replicaBackend,
		clock,
		eviction.NewLRUSet[string](),
		100,
		5*time.Minute,
		replicaErrorLogger)

	t.Run("Put", func(t *testing.T) {
		// Writes should always go to the source backend. There
		// is no point in writing objects to a replica, as it
		// would need to replicate it to the source anyway.
		blobDigest := digest.MustNewDigest("put", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
		sourceBackend.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			})

		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("Get", func(t *testing.T) {
		blobDigest := digest.MustNewDigest("get", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

		// In the initial state, calls to Get() should go to the
		// replica backend, just to see whether it's online.
		clock.EXPECT().Now().Return(time.Unix(10000, 0))
		replicaBackend.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is offline")))
		replicaErrorLogger.EXPECT().Log(status.Error(codes.Unavailable, "Server is offline"))
		clock.EXPECT().Now().Return(time.Unix(10000, 100000000))
		sourceBackend.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is also offline")))

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Source: Server is also offline"), err)

		// If the replica is offline, calls within the next five
		// minutes should be forwarded to the source backend.
		// That way the client doesn't observe too many errors.
		for _, ts := range []int64{10002, 10100, 10200, 10300} {
			clock.EXPECT().Now().Return(time.Unix(ts, 0))
			sourceBackend.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

			data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
		}

		// Once five minutes have passed, the next request is
		// permitted to go to the replica again. If this ends up
		// succeeding, any subsequent request will go to the
		// replica, as it's known to be online.
		for _, ts := range []int64{10301, 10601, 10901, 11201} {
			clock.EXPECT().Now().Return(time.Unix(ts, 0))
			replicaBackend.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
			clock.EXPECT().Now().Return(time.Unix(ts, 100000000))

			data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
		}

		// If a single request to the replica fails, we will
		// fall back to the source for another five minutes.
		clock.EXPECT().Now().Return(time.Unix(11202, 0))
		replicaBackend.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is offline")))
		replicaErrorLogger.EXPECT().Log(status.Error(codes.Unavailable, "Server is offline"))
		clock.EXPECT().Now().Return(time.Unix(11202, 100000000))
		sourceBackend.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is also offline")))

		_, err = blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Source: Server is also offline"), err)

		clock.EXPECT().Now().Return(time.Unix(11501, 0))
		sourceBackend.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is offline")))

		_, err = blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Source: Server is offline"), err)
	})

	t.Run("GetFromComposite", func(t *testing.T) {
		// Don't provide exhaustive testing coverage for
		// GetFromComposite(), as most of the logic is shared
		// with Get(). Just test that traffic is capable of
		// going to both backends.
		parentDigest := digest.MustNewDigest("get-from-composite", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 100)
		childDigest := digest.MustNewDigest("get-from-composite", remoteexecution.DigestFunction_MD5, "80cd354fb9a929ffad1b059d909b3b69", 10)
		slicer := mock.NewMockBlobSlicer(ctrl)

		clock.EXPECT().Now().Return(time.Unix(15000, 0))
		replicaBackend.EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).
			Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is offline")))
		replicaErrorLogger.EXPECT().Log(status.Error(codes.Unavailable, "Server is offline"))
		clock.EXPECT().Now().Return(time.Unix(15000, 100000000))
		sourceBackend.EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).
			Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is also offline")))

		_, err := blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Source: Server is also offline"), err)

		clock.EXPECT().Now().Return(time.Unix(15001, 0))
		sourceBackend.EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).
			Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server is offline")))

		_, err = blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Source: Server is offline"), err)
	})

	t.Run("FindMissing", func(t *testing.T) {
		// Let a FindMissing() call against the replica fail for a
		// given REv2 instance name. This should cause it to be
		// registered as being temporarily unavailable.
		clock.EXPECT().Now().Return(time.Unix(20000, 0))
		replicaBackend.EXPECT().FindMissing(
			ctx,
			digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1).ToSingletonSet(),
		).Return(digest.EmptySet, status.Error(codes.Unavailable, "Server is offline"))
		replicaErrorLogger.EXPECT().Log(status.Error(codes.Unavailable, "Server is offline"))
		clock.EXPECT().Now().Return(time.Unix(20000, 100000000))
		sourceBackend.EXPECT().FindMissing(
			ctx,
			digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1).ToSingletonSet(),
		).Return(digest.EmptySet, nil)

		missing, err := blobAccess.FindMissing(
			ctx,
			digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1).ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)

		// Perform another call with a set that contains digests
		// for multiple instance names. This call should be
		// decomposed, so that errors can be detected at the
		// instance name level.
		//
		// The digests for instance name "find-missing/down"
		// should get forwarded to the source backend, as we
		// observed errors for the replica backend previously.
		clock.EXPECT().Now().Return(time.Unix(20001, 0))
		replicaBackend.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("find-missing/1", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1)).
				Add(digest.MustNewDigest("find-missing/1", remoteexecution.DigestFunction_MD5, "22222222222222222222222222222222", 2)).
				Build(),
		).Return(digest.MustNewDigest("find-missing/1", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1).ToSingletonSet(), nil)
		clock.EXPECT().Now().Return(time.Unix(20001, 100000000)).Times(2)
		replicaBackend.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("find-missing/2", remoteexecution.DigestFunction_MD5, "33333333333333333333333333333333", 3)).
				Add(digest.MustNewDigest("find-missing/2", remoteexecution.DigestFunction_MD5, "44444444444444444444444444444444", 4)).
				Build(),
		).Return(digest.MustNewDigest("find-missing/2", remoteexecution.DigestFunction_MD5, "33333333333333333333333333333333", 3).ToSingletonSet(), nil)
		clock.EXPECT().Now().Return(time.Unix(20001, 100000000)).Times(2)
		sourceBackend.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "55555555555555555555555555555555", 5)).
				Add(digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "66666666666666666666666666666666", 6)).
				Build(),
		).Return(digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "55555555555555555555555555555555", 5).ToSingletonSet(), nil)

		missing, err = blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("find-missing/1", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1)).
				Add(digest.MustNewDigest("find-missing/1", remoteexecution.DigestFunction_MD5, "22222222222222222222222222222222", 2)).
				Add(digest.MustNewDigest("find-missing/2", remoteexecution.DigestFunction_MD5, "33333333333333333333333333333333", 3)).
				Add(digest.MustNewDigest("find-missing/2", remoteexecution.DigestFunction_MD5, "44444444444444444444444444444444", 4)).
				Add(digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "55555555555555555555555555555555", 5)).
				Add(digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "66666666666666666666666666666666", 6)).
				Build())
		require.NoError(t, err)
		require.Equal(
			t,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("find-missing/1", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1)).
				Add(digest.MustNewDigest("find-missing/2", remoteexecution.DigestFunction_MD5, "33333333333333333333333333333333", 3)).
				Add(digest.MustNewDigest("find-missing/down", remoteexecution.DigestFunction_MD5, "55555555555555555555555555555555", 5)).
				Build(),
			missing)
	})
}
