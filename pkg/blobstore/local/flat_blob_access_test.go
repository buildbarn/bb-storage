package local_test

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFlatBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyLocationMap, locationBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
	location1 := local.Location{
		BlockIndex:  7,
		OffsetBytes: 42,
		SizeBytes:   5,
	}
	location2 := local.Location{
		BlockIndex:  8,
		OffsetBytes: 382,
		SizeBytes:   5,
	}

	t.Run("NoRefreshNotFound", func(t *testing.T) {
		// Lookup failures on the blob.
		keyLocationMap.EXPECT().Get(helloKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("NoRefreshSuccess", func(t *testing.T) {
		// The blob is not expected to disappear from storage
		// soon, so no refreshing needs to take place.
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter.Call, false)
		getter.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("RefreshNotFound", func(t *testing.T) {
		// An initial lookup on the blob returned success, but
		// when retrying the lookup while holding an exclusive
		// lock, we got NotFound.
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("RefreshButNoLongerNeeded", func(t *testing.T) {
		// An initial lookup indicated that the blob needed to
		// be refreshed, but a second lookup while holding an
		// exclusive lock showed that this is no longer needed.
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, false)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("RefreshPutFailure", func(t *testing.T) {
		// Refreshing needs to take place, but a failure to
		// allocate space occurs.
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "No space left to store data"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob: No space left to store data"), err)
	})

	t.Run("RefreshFinalizeFailure", func(t *testing.T) {
		// Refreshing needs to take place, but an I/O error
		// takes place while writing the contents.
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return putFinalizer.Call
		})
		putFinalizer.EXPECT().Call().
			Return(local.Location{}, status.Error(codes.Internal, "Write error"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob: Write error"), err)
	})

	t.Run("RefreshSuccess", func(t *testing.T) {
		// Refreshing needs to take place, and it succeeds.
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return putFinalizer.Call
		})
		putFinalizer.EXPECT().Call().
			Return(location2, nil)
		keyLocationMap.EXPECT().Put(helloKey, location2)

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestFlatBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyLocationMap, locationBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
	location := local.Location{
		BlockIndex:  7,
		OffsetBytes: 42,
		SizeBytes:   5,
	}

	t.Run("BrokenBlob", func(t *testing.T) {
		// Calling Put() with a blob that is already in a known
		// error state shouldn't cause any work.
		require.Equal(
			t,
			status.Error(codes.Internal, "Read error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewBufferFromError(status.Error(codes.Internal, "Read error"))))
	})

	t.Run("PutFailure", func(t *testing.T) {
		// A failure while allocating space occurs.
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "No space left to store data"))

		require.Equal(
			t,
			status.Error(codes.Internal, "No space left to store data"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("FinalizeFailure", func(t *testing.T) {
		// An I/O error takes place writing the contents.
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return putFinalizer.Call
		})
		putFinalizer.EXPECT().Call().
			Return(local.Location{}, status.Error(codes.Internal, "Write error"))

		require.Equal(
			t,
			status.Error(codes.Internal, "Write error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("Success", func(t *testing.T) {
		// The blob is written to storage successfully.
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return putFinalizer.Call
		})
		putFinalizer.EXPECT().Call().Return(location, nil)
		keyLocationMap.EXPECT().Put(helloKey, location)

		require.NoError(t, blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestFlatBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyLocationMap, locationBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
	location1 := local.Location{
		BlockIndex:  7,
		OffsetBytes: 42,
		SizeBytes:   5,
	}
	location2 := local.Location{
		BlockIndex:  8,
		OffsetBytes: 382,
		SizeBytes:   5,
	}

	t.Run("Phase1GetFailure", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Disk on fire"), err)
	})

	t.Run("Phase1NotFound", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, helloDigest.ToSingletonSet(), missing)
	})

	t.Run("Phase1Found", func(t *testing.T) {
		getter := mock.NewMockLocationBlobGetter(ctrl)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter.Call, false)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2GetFailure", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Disk on fire"), err)
	})

	t.Run("Phase2PutFailure", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "No space left to store data"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": No space left to store data"), err)
	})

	t.Run("Phase2FinalizeFailure", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return putFinalizer.Call
		})
		putFinalizer.EXPECT().Call().
			Return(local.Location{}, status.Error(codes.Internal, "Write error"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Write error"), err)
	})

	t.Run("Phase2NotFound", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, helloDigest.ToSingletonSet(), missing)
	})

	t.Run("Phase2FoundNoRefresh", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, false)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2FoundRefresh", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(helloKey).
			Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return putFinalizer.Call
		})
		putFinalizer.EXPECT().Call().Return(location2, nil)
		keyLocationMap.EXPECT().Put(helloKey, location2)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})
}
