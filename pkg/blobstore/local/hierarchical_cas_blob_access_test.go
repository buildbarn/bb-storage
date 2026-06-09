package local_test

import (
	"context"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

var (
	brokenChunk = &buffer.Chunk{}
	validChunk  = &buffer.Chunk{}
)

func TestHierarchicalCASBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkCoder := mock.NewMockCoder[*buffer.Chunk, []byte](ctrl)
	chunkCoder.EXPECT().Encode(gomock.Any(), gomock.Any()).DoAndReturn(func(val *buffer.Chunk, d digest.Digest) ([]byte, error) {
		if val == brokenChunk {
			return nil, status.Error(codes.Internal, "Read error")
		}
		return []byte("Hello"), nil
	}).AnyTimes()
	chunkCoder.EXPECT().Decode(gomock.Any(), gomock.Any()).DoAndReturn(func(data []byte, d digest.Digest) (*buffer.Chunk, error) {
		if string(data) == "error" {
			return nil, status.Error(codes.Internal, "Read error")
		}
		return validChunk, nil
	}).AnyTimes()

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	blockReferenceResolver := mock.NewMockBlockReferenceResolver(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewHierarchicalCSBlobAccess(keyLocationMap, blockReferenceResolver, locationBlobMap, &sync.RWMutex{}, capabilitiesProvider, chunkCoder)

	helloDigest := digest.MustNewDigest("some/instance/name", remoteexecution.DigestFunction_SHA256, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	lookupKey1 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-")
	lookupKey2 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some")
	lookupKey3 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance")
	lookupKey4 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name")
	canonicalKey := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
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
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey3, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey4, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		_, err := blobAccess.Get(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	t.Run("NoRefreshFailure", func(t *testing.T) {
		// Errors other than NotFound should be propagated.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.Get(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)
	})

	t.Run("NoRefreshSuccess", func(t *testing.T) {
		// The blob is not expected to disappear from storage
		// soon, so no refreshing needs to take place.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, false)
		getter.EXPECT().Call(helloDigest).
			Return([]byte("Hello"), func() {}, nil)

		chunk, err := blobAccess.Get(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, validChunk, chunk)
	})

	t.Run("RefreshSyncWithCanonical", func(t *testing.T) {
		// The lookup entry in the key-location map needs to be
		// refreshed. Fortunately, the canonical entry in the
		// key-location map doesn't need to be refreshed, so we
		// can simply copy that entry and return the contents of
		// that object.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)
		getter1.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)
		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).Return(location2, nil)

		getter3 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).Return(getter3.Call, false)
		keyLocationMap.EXPECT().Put(lookupKey1, location2, blockReferenceResolver)

		chunk, err := blobAccess.Get(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, validChunk, chunk)
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
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)
		getter1.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)
		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call([]byte("Hello")).Return(putFinalizer.Call)
		putFinalizer.EXPECT().Call().Return(location2, nil)

		keyLocationMap.EXPECT().Put(canonicalKey, location2, blockReferenceResolver)
		keyLocationMap.EXPECT().Put(lookupKey1, location2, blockReferenceResolver)

		chunk, err := blobAccess.Get(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, validChunk, chunk)
	})
}

func TestHierarchicalCASBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkCoder := mock.NewMockCoder[*buffer.Chunk, []byte](ctrl)
	chunkCoder.EXPECT().Encode(gomock.Any(), gomock.Any()).DoAndReturn(func(val *buffer.Chunk, d digest.Digest) ([]byte, error) {
		if val == brokenChunk {
			return nil, status.Error(codes.Internal, "Read error")
		}
		return []byte("Hello"), nil
	}).AnyTimes()
	chunkCoder.EXPECT().Decode(gomock.Any(), gomock.Any()).DoAndReturn(func(data []byte, d digest.Digest) (*buffer.Chunk, error) {
		if string(data) == "error" {
			return nil, status.Error(codes.Internal, "Read error")
		}
		return validChunk, nil
	}).AnyTimes()

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	blockReferenceResolver := mock.NewMockBlockReferenceResolver(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewHierarchicalCSBlobAccess(keyLocationMap, blockReferenceResolver, locationBlobMap, &sync.RWMutex{}, capabilitiesProvider, chunkCoder)

	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	canonicalKey := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
	mostSpecificLookupKey := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-example")
	location1 := local.Location{
		BlockIndex:  7,
		OffsetBytes: 42,
		SizeBytes:   5,
	}

	t.Run("BrokenBlob", func(t *testing.T) {
		// Calling Put() with a blob that simulates a coder
		// error shouldn't cause any work on the map layers.
		require.Equal(
			t,
			status.Error(codes.Internal, "Read error"),
			blobAccess.Put(ctx, helloDigest, brokenChunk),
		)
	})

	t.Run("CanonicalLookupFailure", func(t *testing.T) {
		// To prevent redundant storage of the same object, we
		// always do a lookup to see if an object with the same
		// contents already exists. Let that lookup fail.
		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.Internal, "Disk failure"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Disk failure"),
			blobAccess.Put(
				ctx,
				helloDigest,
				validChunk,
			),
		)
	})

	t.Run("CanonicalLookupSuccess", func(t *testing.T) {
		// Successfully create an object that already exists for
		// a different instance name. We don't ingest the data,
		// but simply create an additional entry for it in the
		// key-location map.
		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, false)
		keyLocationMap.EXPECT().Put(mostSpecificLookupKey, location1, blockReferenceResolver)

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				helloDigest,
				validChunk,
			),
		)
	})

	t.Run("SpaceAllocationFailure", func(t *testing.T) {
		// In case the object isn't present yet, we should try
		// to allocate space to store a copy. Let allocation fail.
		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		locationBlobMap.EXPECT().Put(int64(5)).Return(nil, status.Error(codes.Internal, "Disk failure"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Disk failure"),
			blobAccess.Put(
				ctx,
				helloDigest,
				validChunk,
			),
		)
	})

	t.Run("FinalizeFailure", func(t *testing.T) {
		// Let space allocation succeed, but the completion of
		// data finalization fail (e.g. disk write failure).
		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)

		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call([]byte("Hello")).Return(putFinalizer.Call)
		putFinalizer.EXPECT().Call().Return(local.Location{}, status.Error(codes.Canceled, "Call canceled by client"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Canceled, "Call canceled by client"),
			blobAccess.Put(
				ctx,
				helloDigest,
				validChunk,
			),
		)
	})

	t.Run("Success", func(t *testing.T) {
		// Let ingestion of data succeed. Upon success, two
		// key-location map entries should be written. One that
		// purely identifies the contents, and one that contains
		// the instance name.
		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)

		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call([]byte("Hello")).Return(putFinalizer.Call)
		putFinalizer.EXPECT().Call().Return(location1, nil)

		keyLocationMap.EXPECT().Put(canonicalKey, location1, blockReferenceResolver)
		keyLocationMap.EXPECT().Put(mostSpecificLookupKey, location1, blockReferenceResolver)

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				helloDigest,
				validChunk,
			),
		)
	})
}

func TestHierarchicalCASBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkCoder := mock.NewMockCoder[*buffer.Chunk, []byte](ctrl)
	chunkCoder.EXPECT().Encode(gomock.Any(), gomock.Any()).DoAndReturn(func(val *buffer.Chunk, d digest.Digest) ([]byte, error) {
		if val == brokenChunk {
			return nil, status.Error(codes.Internal, "Read error")
		}
		return []byte("Hello"), nil
	}).AnyTimes()
	chunkCoder.EXPECT().Decode(gomock.Any(), gomock.Any()).DoAndReturn(func(data []byte, d digest.Digest) (*buffer.Chunk, error) {
		if string(data) == "error" {
			return nil, status.Error(codes.Internal, "Read error")
		}
		return validChunk, nil
	}).AnyTimes()

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	blockReferenceResolver := mock.NewMockBlockReferenceResolver(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	blobAccess := local.NewHierarchicalCSBlobAccess(keyLocationMap, blockReferenceResolver, locationBlobMap, &sync.RWMutex{}, capabilitiesProvider, chunkCoder)

	helloDigest := digest.MustNewDigest("some/instance/name", remoteexecution.DigestFunction_SHA256, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
	lookupKey1 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-")
	lookupKey2 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some")
	lookupKey3 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance")
	lookupKey4 := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name")
	canonicalKey := local.NewKeyFromString("1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5")
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
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase1NotFound", func(t *testing.T) {
		// Only after scanning the key-location map for all four
		// instance names should we return a NotFound error.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey3, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey4, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, helloDigest.ToSingletonSet(), missing)
	})

	t.Run("Phase1Found", func(t *testing.T) {
		// We may stop scanning the key-location map once we've
		// found a valid entry.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey3, blockReferenceResolver).Return(location1, nil)
		getter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter.Call, false)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2GetLookupFailure", func(t *testing.T) {
		// Failure to read the key-location map entry during a
		// refresh. The error should be propagated.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to get blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2GetCanonicalFailure", func(t *testing.T) {
		// Successfully read the key-location map entry for the
		// object itself, but failed to read the canonical entry
		// that is also stored in the key-location map.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).Return(location1, nil)

		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2SyncWithCanonicalFailure", func(t *testing.T) {
		// The lookup entry needs to be refreshed, while the
		// canonical entry doesn't. This should trigger a simple
		// copy of the canonical entry. Let that fail.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)

		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).Return(location2, nil)
		getter3 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).Return(getter3.Call, false)
		keyLocationMap.EXPECT().Put(lookupKey2, location2, blockReferenceResolver).
			Return(status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2SyncWithCanonicalSuccess", func(t *testing.T) {
		// Let copying of the canonical entry succeed.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)

		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).Return(location2, nil)
		getter3 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location2).Return(getter3.Call, false)
		keyLocationMap.EXPECT().Put(lookupKey2, location2, blockReferenceResolver)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("Phase2RefreshFailure1", func(t *testing.T) {
		// Simulate the case where we need to refresh the
		// object, but fail to allocate space for it.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)

		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		locationBlobMap.EXPECT().Put(int64(5)).
			Return(nil, status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2FinalizeFailure", func(t *testing.T) {
		// Let space allocation succeed, but the copying from old to new fails.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)

		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call([]byte("Hello")).Return(putFinalizer.Call)
		putFinalizer.EXPECT().Call().Return(local.Location{}, status.Error(codes.Canceled, "Call canceled by client"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Canceled, "Failed to refresh blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Call canceled by client"), err)
	})

	t.Run("Phase2RefreshFailure3", func(t *testing.T) {
		// Copying succeeds, but we can't write the updated
		// key-location map entry.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)

		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call([]byte("Hello")).Return(putFinalizer.Call)
		putFinalizer.EXPECT().Call().Return(location2, nil)

		keyLocationMap.EXPECT().Put(canonicalKey, location2, blockReferenceResolver).
			Return(status.Error(codes.Internal, "Disk on fire"))

		_, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to refresh blob \"1-185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5-some/instance/name\": Disk on fire"), err)
	})

	t.Run("Phase2RefreshSuccess", func(t *testing.T) {
		// Let copying of the data and writing of both the
		// canonical and lookup key-location map entries
		// succeed. The blob should be reported as present.
		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter1 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter1.Call, true)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)
		getter2 := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location1).Return(getter2.Call, true)
		getter2.EXPECT().Call(helloDigest).Return([]byte("Hello"), func() {}, nil)

		keyLocationMap.EXPECT().Get(lookupKey1, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))
		keyLocationMap.EXPECT().Get(lookupKey2, blockReferenceResolver).Return(location1, nil)

		keyLocationMap.EXPECT().Get(canonicalKey, blockReferenceResolver).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		putWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(5)).Return(putWriter.Call, nil)
		putFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		putWriter.EXPECT().Call([]byte("Hello")).Return(putFinalizer.Call)
		putFinalizer.EXPECT().Call().Return(location2, nil)

		keyLocationMap.EXPECT().Put(canonicalKey, location2, blockReferenceResolver)
		keyLocationMap.EXPECT().Put(lookupKey2, location2, blockReferenceResolver)

		missing, err := blobAccess.FindMissing(ctx, helloDigest.ToSingletonSet())
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})
}
