package local_test

import (
	"context"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestFlatBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyLocationMap, locationBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
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

func TestFlatBlobAccessGetFromComposite(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyLocationMap, locationBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	parentDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)
	parentKey := local.NewKeyFromString("3-3e25960a79dbc69b674cd4ec67a72c62-11")
	child1Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	child1Key := local.NewKeyFromString("3-8b1a9953c4611296a827abf8c47804d7-5")
	child2Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "7d793037a0760186574b0282f2f435e7", 5)
	child2Key := local.NewKeyFromString("3-7d793037a0760186574b0282f2f435e7-5")
	slicer := mock.NewMockBlobSlicer(ctrl)
	location1 := local.Location{
		BlockIndex:  7,
		OffsetBytes: 42,
		SizeBytes:   11,
	}
	location2 := local.Location{
		BlockIndex:  8,
		OffsetBytes: 382,
		SizeBytes:   5,
	}

	t.Run("NoSlicingParentNotFound", func(t *testing.T) {
		// Even if the requested child object were to exist, the
		// existence of the parent object is the one that
		// counts. If the parent object does not exist, we
		// should report it as being absent, so that it may be
		// replicated/regenerated.
		keyLocationMap.EXPECT().Get(parentKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))

		_, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("NoSlicingChildFailure", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(local.Location{}, status.Error(codes.Internal, "I/O error"))

		_, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("NoSlicingSuccess", func(t *testing.T) {
		// If both the parent doesn't need to be refreshed and
		// the child exists, we can return data immediately.
		// Don't bother taking into account whether the child
		// needs to be refreshed. We'll just slice the object
		// again by the time the child gets lost.
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(location2, nil)
		childGetter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).
			Return(childGetter.Call, true)
		childGetter.EXPECT().Call(child1Digest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SlicingParentNotFound", func(t *testing.T) {
		// An initial lookup on the parent returned success, but
		// when retrying the lookup while holding an exclusive
		// lock, we got NotFound.
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(getter.Call, true)
		keyLocationMap.EXPECT().Get(parentKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))

		_, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("SlicingChildFailure", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter1.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter2.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(local.Location{}, status.Error(codes.Internal, "I/O error"))

		_, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "I/O error"), err)
	})

	t.Run("SlicingButNoLongerNeeded", func(t *testing.T) {
		// An initial lookup indicated that the parent needed to
		// be sliced, but a second lookup while holding an
		// exclusive lock showed that this is no longer needed.
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter1.Call, true)
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter2.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(location2, nil)
		childGetter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).
			Return(childGetter.Call, true)
		childGetter.EXPECT().Call(child1Digest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SlicingPutFailure", func(t *testing.T) {
		// Successfully sliced an object, but we failed to
		// insert new key-location map entries for the resulting
		// slices.
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter1.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter2.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))
		parentGetter2.EXPECT().Call(parentDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		slicer.EXPECT().Slice(gomock.Any(), child1Digest).DoAndReturn(func(b buffer.Buffer, child1Digest digest.Digest) (buffer.Buffer, []slicing.BlobSlice) {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), data)
			return buffer.NewValidatedBufferFromByteSlice([]byte("Hello")), []slicing.BlobSlice{
				{Digest: child1Digest, OffsetBytes: 0, SizeBytes: 5},
				{Digest: child2Digest, OffsetBytes: 6, SizeBytes: 5},
			}
		})
		keyLocationMap.EXPECT().Put(child1Key, local.Location{
			BlockIndex:  7,
			OffsetBytes: 42,
			SizeBytes:   5,
		}).Return(status.Error(codes.Internal, "I/O error"))

		_, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to create child blob \"3-8b1a9953c4611296a827abf8c47804d7-5-example\": I/O error"), err)
	})

	t.Run("SlicingSuccess", func(t *testing.T) {
		// Successful instance where an object is sliced, and the
		// contents of a single slice is returned.
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter1.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter2.Call, false)
		keyLocationMap.EXPECT().Get(child1Key).
			Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))
		parentGetter2.EXPECT().Call(parentDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		slicer.EXPECT().Slice(gomock.Any(), child1Digest).DoAndReturn(func(b buffer.Buffer, child1Digest digest.Digest) (buffer.Buffer, []slicing.BlobSlice) {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), data)
			return buffer.NewValidatedBufferFromByteSlice([]byte("Hello")), []slicing.BlobSlice{
				{Digest: child1Digest, OffsetBytes: 0, SizeBytes: 5},
				{Digest: child2Digest, OffsetBytes: 6, SizeBytes: 5},
			}
		})
		keyLocationMap.EXPECT().Put(child1Key, local.Location{
			BlockIndex:  7,
			OffsetBytes: 42,
			SizeBytes:   5,
		})
		keyLocationMap.EXPECT().Put(child2Key, local.Location{
			BlockIndex:  7,
			OffsetBytes: 48,
			SizeBytes:   5,
		})

		data, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("RefreshSuccess", func(t *testing.T) {
		// Successful instance where an object is both refreshed and
		// sliced, and the contents of a single slice is returned.
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter1.Call, true)
		keyLocationMap.EXPECT().Get(parentKey).
			Return(location1, nil)
		parentGetter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).
			Return(parentGetter2.Call, true)
		parentGetter2.EXPECT().Call(parentDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(11)).
			Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), data)
			return putFinalizer.Call
		})
		putFinalizer.EXPECT().Call().
			Return(location2, nil)
		keyLocationMap.EXPECT().Put(parentKey, location2)
		slicer.EXPECT().Slice(gomock.Any(), child1Digest).DoAndReturn(func(b buffer.Buffer, child1Digest digest.Digest) (buffer.Buffer, []slicing.BlobSlice) {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), data)
			return buffer.NewValidatedBufferFromByteSlice([]byte("Hello")), []slicing.BlobSlice{
				{Digest: child1Digest, OffsetBytes: 0, SizeBytes: 5},
				{Digest: child2Digest, OffsetBytes: 6, SizeBytes: 5},
			}
		})
		keyLocationMap.EXPECT().Put(child1Key, local.Location{
			BlockIndex:  8,
			OffsetBytes: 382,
			SizeBytes:   5,
		})
		keyLocationMap.EXPECT().Put(child2Key, local.Location{
			BlockIndex:  8,
			OffsetBytes: 388,
			SizeBytes:   5,
		})

		data, err := blobAccess.GetFromComposite(ctx, parentDigest, child1Digest, slicer).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	// TODO: Some testing coverage may be missing for the cases
	// where refreshing fails.
}

func TestFlatBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewFlatBlobAccess(keyLocationMap, locationBlobMap, digest.KeyWithoutInstance, &sync.RWMutex{}, "cas", capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
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
	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	helloKey := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
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
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Disk on fire"), err)
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
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Disk on fire"), err)
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
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": No space left to store data"), err)
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
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example\": Write error"), err)
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
