package local_test

import (
	"context"
	"io"
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

func TestHierarchicalCASBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewHierarchicalCASBlobAccess(keyLocationMap, locationBlobMap, &sync.RWMutex{}, capabilitiesProvider)
	helloDigest := digest.MustNewDigest("some/instance/name", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	lookupKey1 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-")
	lookupKey2 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some")
	lookupKey3 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance")
	lookupKey4 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name")
	canonicalKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
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
		// Multiple lookups should be performed against the
		// key-location map. Let each of those fail.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey3).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey4).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	t.Run("NoRefreshFailure", func(t *testing.T) {
		// Errors other than NotFound should be propagated.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)
	})

	t.Run("NoRefreshSuccess", func(t *testing.T) {
		// The blob is not expected to disappear from storage
		// soon, so no refreshing needs to take place.
		keyLocationMap.EXPECT().Get(lookupKey1).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, false)
		reader := mock.NewMockReadCloser(ctrl)
		getter.EXPECT().Call(helloDigest).
			Return(buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided))
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
		reader.EXPECT().Close()

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("RefreshSyncWithCanonical", func(t *testing.T) {
		// The lookup entry in the key-location map needs to be
		// refreshed. Fortunately, the canonical entry in the
		// key-location map doesn't need to be refreshed, so we
		// can simply copy that entry and return the contents of
		// that object.
		keyLocationMap.EXPECT().Get(lookupKey1).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(lookupKey1).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).Return(location2, nil)
		getter3 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).Return(getter3.Call, false)
		keyLocationMap.EXPECT().Put(lookupKey1, location2)
		reader := mock.NewMockReadCloser(ctrl)
		getter3.EXPECT().Call(helloDigest).
			Return(buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided))
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
		reader.EXPECT().Close()

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("RefreshSuccess", func(t *testing.T) {
		// Because there is no canonical entry in the
		// key-location map that doesn't need to be refreshed,
		// we must do a copy of the object in the location-blob
		// map to ensure it doesn't disappear from storage.
		//
		// Upon completion, both key-location map entries for
		// the lookup key and the canonical key need to be
		// updated.
		keyLocationMap.EXPECT().Get(lookupKey1).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)
		keyLocationMap.EXPECT().Get(lookupKey1).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		reader := mock.NewMockReadCloser(ctrl)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
		reader.EXPECT().Close()
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return func() (local.Location, error) {
				return location2, nil
			}
		})
		keyLocationMap.EXPECT().Put(canonicalKey, location2)
		keyLocationMap.EXPECT().Put(lookupKey1, location2)

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestHierarchicalCASBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewHierarchicalCASBlobAccess(keyLocationMap, locationBlobMap, &sync.RWMutex{}, capabilitiesProvider)
	helloDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	canonicalKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
	mostSpecificLookupKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example")
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

	t.Run("BrokenBlob", func(t *testing.T) {
		// Calling Put() with a blob that is already in a known
		// error state shouldn't cause any work.
		require.Equal(
			t,
			status.Error(codes.Internal, "Read error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewBufferFromError(status.Error(codes.Internal, "Read error"))))
	})

	t.Run("CanonicalLookupFailure", func(t *testing.T) {
		// To prevent redundant storage of the same object, we
		// always do a lookup to see if an object with the same
		// contents already exists. Let that lookup fail.
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.Internal, "Disk failure"))
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Close()

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Disk failure"),
			blobAccess.Put(
				ctx,
				helloDigest,
				buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided)))
	})

	t.Run("CanonicalLookupValidBrokenBlob", func(t *testing.T) {
		// In case we already have the blob, we shouldn't
		// attempt to store it again. We should still read the
		// contents provided by the caller and validate them.
		// This ensures that the client isn't capable of gaining
		// access to arbitrary blobs.
		keyLocationMap.EXPECT().Get(canonicalKey).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, false)
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Xyzzy"), io.EOF
		})
		reader.EXPECT().Close()

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Buffer has checksum 7609128715518308672067aab169e24944ead24e3d732aab8a8f0b7013a65564, while 185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969 was expected"),
			blobAccess.Put(
				ctx,
				helloDigest,
				buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided)))
	})

	t.Run("CanonicalLookupSuccess", func(t *testing.T) {
		// Successfully create an object that already exists for
		// a different instance name. We don't ingest the data,
		// but simply create an additional entry for it in the
		// key-location map.
		keyLocationMap.EXPECT().Get(canonicalKey).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, false)
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
		reader.EXPECT().Close()
		keyLocationMap.EXPECT().Get(canonicalKey).Return(location2, nil)
		keyLocationMap.EXPECT().Put(mostSpecificLookupKey, location2)

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				helloDigest,
				buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided)))
	})

	t.Run("SpaceAllocationFailure", func(t *testing.T) {
		// In case the object isn't present yet, we should try
		// to allocate space to store a copy. Let allocation fail.
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		locationBlobMap.EXPECT().Put(int64(5)).Return(nil, status.Error(codes.Internal, "Disk failure"))
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Close()

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Disk failure"),
			blobAccess.Put(
				ctx,
				helloDigest,
				buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided)))
	})

	t.Run("ReadFailure", func(t *testing.T) {
		// Let space allocation succeed, but the ingestion of
		// data fail. This should not cause us to write any
		// key-location map entries.
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Canceled, "Call canceled by client"))
		reader.EXPECT().Close()
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			_, err := b.ToByteSlice(1000)
			testutil.RequireEqualStatus(t, status.Error(codes.Canceled, "Call canceled by client"), err)
			return func() (local.Location, error) {
				return local.Location{}, err
			}
		})

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Canceled, "Call canceled by client"),
			blobAccess.Put(
				ctx,
				helloDigest,
				buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided)))
	})

	t.Run("Success", func(t *testing.T) {
		// Let ingestion of data succeed. Upon success, two
		// key-location map entries should be written. One that
		// purely identifies the contents, and one that contains
		// the instance name.
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		reader := mock.NewMockReadCloser(ctrl)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
		reader.EXPECT().Close()
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return func() (local.Location, error) {
				return location1, nil
			}
		})
		keyLocationMap.EXPECT().Put(canonicalKey, location1)
		keyLocationMap.EXPECT().Put(mostSpecificLookupKey, location1)

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				helloDigest,
				buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided)))
	})
}

func TestHierarchicalCASBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewHierarchicalCASBlobAccess(keyLocationMap, locationBlobMap, &sync.RWMutex{}, capabilitiesProvider)
	helloDigest := digest.MustNewDigest("some/instance/name", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	lookupKey1 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-")
	lookupKey2 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some")
	lookupKey3 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance")
	lookupKey4 := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name")
	canonicalKey := local.NewKeyFromString("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
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
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase1NotFound", func(t *testing.T) {
		// Only after scanning the key-location map for all four
		// instance names should we return a NotFound error.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey3).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey4).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, helloDigest.ToSingletonSet(), missing)
	})

	t.Run("Phase1Found", func(t *testing.T) {
		// We may stop scanning the key-location map once we've
		// found a valid entry.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey3).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, false)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2GetLookupFailure", func(t *testing.T) {
		// Failure to read the key-location map entry during a
		// refresh. The error should be propagated.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2GetCanonicalFailure", func(t *testing.T) {
		// Successfully read the key-location map entry for the
		// object itself, but failed to read the canonical entry
		// that is also stored in the key-location map.
		keyLocationMap.EXPECT().Get(lookupKey1).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2SyncWithCanonicalFailure", func(t *testing.T) {
		// The lookup entry needs to be refreshed, while the
		// canonical entry doesn't. This should trigger a simple
		// copy of the canonical entry. Let that fail.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).Return(location2, nil)
		getter3 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).Return(getter3.Call, false)
		keyLocationMap.EXPECT().Put(lookupKey2, location2).
			Return(status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2SyncWithCanonicalSuccess", func(t *testing.T) {
		// Let copying of the canonical entry succeed.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).Return(location2, nil)
		getter3 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).Return(getter3.Call, false)
		keyLocationMap.EXPECT().Put(lookupKey2, location2)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2RefreshFailure1", func(t *testing.T) {
		// Simulate the case where we need to refresh the
		// object, but fail to allocate space for it.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		reader := mock.NewMockReadCloser(ctrl)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided))
		locationBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "Disk on fire"))
		reader.EXPECT().Close()

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2RefreshFailure2", func(t *testing.T) {
		// Let copying of the object from old to new fail.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		reader := mock.NewMockReadCloser(ctrl)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		reader.EXPECT().Read(gomock.Any()).Return(0, status.Error(codes.Canceled, "Call canceled by client"))
		reader.EXPECT().Close()
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			_, err := b.ToByteSlice(1000)
			testutil.RequireEqualStatus(t, status.Error(codes.Canceled, "Call canceled by client"), err)
			return func() (local.Location, error) {
				return local.Location{}, err
			}
		})

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Canceled, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Call canceled by client"), err)
	})

	t.Run("Phase2RefreshFailure3", func(t *testing.T) {
		// Copying succeeds, but we can't write the updated
		// key-location map entry.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		reader := mock.NewMockReadCloser(ctrl)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
		reader.EXPECT().Close()
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return func() (local.Location, error) {
				return location2, nil
			}
		})
		keyLocationMap.EXPECT().Put(canonicalKey, location2).
			Return(status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2RefreshSuccess", func(t *testing.T) {
		// Let copying of the data and writing of both the
		// canonical and lookup key-location map entries
		// succeed. The blob should be reported as present.
		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		keyLocationMap.EXPECT().Get(canonicalKey).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		reader := mock.NewMockReadCloser(ctrl)
		getter2.EXPECT().Call(helloDigest).
			Return(buffer.NewCASBufferFromReader(helloDigest, reader, buffer.UserProvided))
		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		reader.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return copy(p, "Hello"), io.EOF
		})
		reader.EXPECT().Close()
		putWriter.EXPECT().Call(gomock.Any()).DoAndReturn(func(b buffer.Buffer) local.LocationBlobPutFinalizer {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return func() (local.Location, error) {
				return location2, nil
			}
		})
		keyLocationMap.EXPECT().Put(canonicalKey, location2)
		keyLocationMap.EXPECT().Put(lookupKey2, location2)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})
}
