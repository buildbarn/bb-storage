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

	keyBlobMap := mock.NewMockKeyBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")

	t.Run("NoRefreshNotFound", func(t *testing.T) {
		// Lookup failures on the blob.
		keyBlobMap.EXPECT().Get(helloKey).
			Return(nil, int64(0), false, status.Error(codes.NotFound, "Blob not found"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("NoRefreshSuccess", func(t *testing.T) {
		// The blob is not expected to disappear from storage
		// soon, so no refreshing needs to take place.
		keyBlobGetter := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter.Call, int64(5), false, nil)
		keyBlobGetter.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("RefreshNotFound", func(t *testing.T) {
		// An initial lookup on the blob returned success, but
		// when retrying the lookup while holding an exclusive
		// lock, we got NotFound.
		keyBlobGetter := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter.Call, int64(5), true, nil)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(nil, int64(0), false, status.Error(codes.NotFound, "Blob not found"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("RefreshButNoLongerNeeded", func(t *testing.T) {
		// An initial lookup indicated that the blob needed to
		// be refreshed, but a second lookup while holding an
		// exclusive lock showed that this is no longer needed.
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), false, nil)
		keyBlobGetter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("RefreshPutFailure", func(t *testing.T) {
		// Refreshing needs to take place, but a failure to
		// allocate space occurs.
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), true, nil)
		keyBlobGetter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "No space left to store data"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob: No space left to store data"), err)
	})

	t.Run("RefreshFinalizeFailure", func(t *testing.T) {
		// Refreshing needs to take place, but an I/O error
		// takes place while writing the contents.
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), true, nil)
		keyBlobGetter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		keyBlobPutWriter := mock.NewMockKeyBlobPutWriter(ctrl)
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(keyBlobPutWriter.Call, nil)
		keyBlobPutFinalizer := mock.NewMockKeyBlobPutFinalizer(ctrl)
		keyBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.KeyBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return keyBlobPutFinalizer.Call
		})
		keyBlobPutFinalizer.EXPECT().Call(helloKey).
			Return(status.Error(codes.Internal, "Write error"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob: Write error"), err)
	})

	t.Run("RefreshSuccess", func(t *testing.T) {
		// Refreshing needs to take place, and it succeeds.
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), true, nil)
		keyBlobGetter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		keyBlobPutWriter := mock.NewMockKeyBlobPutWriter(ctrl)
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(keyBlobPutWriter.Call, nil)
		keyBlobPutFinalizer := mock.NewMockKeyBlobPutFinalizer(ctrl)
		keyBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.KeyBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return keyBlobPutFinalizer.Call
		})
		keyBlobPutFinalizer.EXPECT().Call(helloKey)

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestFlatBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyBlobMap := mock.NewMockKeyBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")

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
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "No space left to store data"))

		require.Equal(
			t,
			status.Error(codes.Internal, "No space left to store data"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("FinalizeFailure", func(t *testing.T) {
		// An I/O error takes place writing the contents.
		keyBlobPutWriter := mock.NewMockKeyBlobPutWriter(ctrl)
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(keyBlobPutWriter.Call, nil)
		keyBlobPutFinalizer := mock.NewMockKeyBlobPutFinalizer(ctrl)
		keyBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.KeyBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return keyBlobPutFinalizer.Call
		})
		keyBlobPutFinalizer.EXPECT().Call(helloKey).
			Return(status.Error(codes.Internal, "Write error"))

		require.Equal(
			t,
			status.Error(codes.Internal, "Write error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("Success", func(t *testing.T) {
		// The blob is written to storage successfully.
		keyBlobPutWriter := mock.NewMockKeyBlobPutWriter(ctrl)
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(keyBlobPutWriter.Call, nil)
		keyBlobPutFinalizer := mock.NewMockKeyBlobPutFinalizer(ctrl)
		keyBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.KeyBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return keyBlobPutFinalizer.Call
		})
		keyBlobPutFinalizer.EXPECT().Call(helloKey)

		require.NoError(t, blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestFlatBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyBlobMap := mock.NewMockKeyBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")

	t.Run("Phase1GetFailure", func(t *testing.T) {
		keyBlobMap.EXPECT().Get(helloKey).
			Return(nil, int64(0), false, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Disk on fire"), err)
	})

	t.Run("Phase1NotFound", func(t *testing.T) {
		keyBlobMap.EXPECT().Get(helloKey).
			Return(nil, int64(0), false, status.Error(codes.NotFound, "Object not found"))

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, helloDigest.ToSingletonSet(), missing)
	})

	t.Run("Phase1Found", func(t *testing.T) {
		keyBlobGetter := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter.Call, int64(5), false, nil)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2GetFailure", func(t *testing.T) {
		keyBlobGetter := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter.Call, int64(5), true, nil)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(nil, int64(0), false, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Disk on fire"), err)
	})

	t.Run("Phase2PutFailure", func(t *testing.T) {
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), true, nil)
		keyBlobGetter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "No space left to store data"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": No space left to store data"), err)
	})

	t.Run("Phase2FinalizeFailure", func(t *testing.T) {
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), true, nil)
		keyBlobGetter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		keyBlobPutWriter := mock.NewMockKeyBlobPutWriter(ctrl)
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(keyBlobPutWriter.Call, nil)
		keyBlobPutFinalizer := mock.NewMockKeyBlobPutFinalizer(ctrl)
		keyBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.KeyBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return keyBlobPutFinalizer.Call
		})
		keyBlobPutFinalizer.EXPECT().Call(helloKey).
			Return(status.Error(codes.Internal, "Write error"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Write error"), err)
	})

	t.Run("Phase2NotFound", func(t *testing.T) {
		keyBlobGetter := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter.Call, int64(5), true, nil)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(nil, int64(0), false, status.Error(codes.NotFound, "Object not found"))

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, helloDigest.ToSingletonSet(), missing)
	})

	t.Run("Phase2FoundNoRefresh", func(t *testing.T) {
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), false, nil)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2FoundRefresh", func(t *testing.T) {
		keyBlobGetter1 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter1.Call, int64(5), true, nil)
		keyBlobGetter2 := mock.NewMockKeyBlobGetter(ctrl)
		keyBlobMap.EXPECT().Get(helloKey).
			Return(keyBlobGetter2.Call, int64(5), true, nil)
		keyBlobGetter2.EXPECT().Call(helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		keyBlobPutWriter := mock.NewMockKeyBlobPutWriter(ctrl)
		keyBlobMap.EXPECT().Put(int64(5)).
			Return(keyBlobPutWriter.Call, nil)
		keyBlobPutFinalizer := mock.NewMockKeyBlobPutFinalizer(ctrl)
		keyBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.KeyBlobPutFinalizer {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return keyBlobPutFinalizer.Call
		})
		keyBlobPutFinalizer.EXPECT().Call(helloKey)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})
}
