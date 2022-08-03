package local_test

import (
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

func TestLocationBasedKeyBlobMap(t *testing.T) {
	ctrl := gomock.NewController(t)

	keyLocationMap := mock.NewMockKeyLocationMap(ctrl)
	locationBlobMap := mock.NewMockLocationBlobMap(ctrl)
	keyBlobMap := local.NewLocationBasedKeyBlobMap(keyLocationMap, locationBlobMap)

	key := local.NewKeyFromString("8b1a9953c4611296a827abf8c47804d7-5-example")
	blobDigest := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)
	location := local.Location{
		BlockIndex:  12,
		OffsetBytes: 42,
		SizeBytes:   5,
	}

	t.Run("GetKeyLocationMapError", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(key).
			Return(local.Location{}, status.Error(codes.NotFound, "Object not found"))

		_, _, _, err := keyBlobMap.Get(key)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	t.Run("GetLocationBlobMapError", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(key).Return(location, nil)
		locationBlobGetter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location).Return(locationBlobGetter.Call, false)
		locationBlobGetter.EXPECT().Call(blobDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Disk on fire")))

		keyBlobGetter, sizeBytes, needsRefresh, err := keyBlobMap.Get(key)
		require.NoError(t, err)
		require.Equal(t, int64(5), sizeBytes)
		require.False(t, needsRefresh)

		_, err = keyBlobGetter(blobDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)
	})

	t.Run("GetSuccess", func(t *testing.T) {
		keyLocationMap.EXPECT().Get(key).Return(location, nil)
		locationBlobGetter := mock.NewMockLocationBlobGetter(ctrl)
		locationBlobMap.EXPECT().Get(location).Return(locationBlobGetter.Call, true)
		locationBlobGetter.EXPECT().Call(blobDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		keyBlobGetter, sizeBytes, needsRefresh, err := keyBlobMap.Get(key)
		require.NoError(t, err)
		require.Equal(t, int64(5), sizeBytes)
		require.True(t, needsRefresh)

		data, err := keyBlobGetter(blobDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("PutLocationBlobMapError1", func(t *testing.T) {
		locationBlobMap.EXPECT().Put(int64(123)).
			Return(nil, status.Error(codes.Internal, "No space left"))

		_, err := keyBlobMap.Put(123)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "No space left"), err)
	})

	t.Run("PutLocationBlobMapError2", func(t *testing.T) {
		locationBlobPutWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(123)).Return(locationBlobPutWriter.Call, nil)
		locationBlobPutFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		locationBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
			func(b buffer.Buffer) local.LocationBlobPutFinalizer {
				_, err := b.ToByteSlice(10)
				testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Client hung up"), err)
				return locationBlobPutFinalizer.Call
			})
		locationBlobPutFinalizer.EXPECT().Call().
			Return(local.Location{}, status.Error(codes.Unknown, "Failed to read data: Client hung up"))

		keyBlobPutWriter, err := keyBlobMap.Put(123)
		require.NoError(t, err)
		require.Equal(
			t,
			status.Error(codes.Unknown, "Failed to read data: Client hung up"),
			keyBlobPutWriter(buffer.NewBufferFromError(status.Error(codes.Unknown, "Client hung up")))(key))
	})

	t.Run("PutKeyLocationMapError", func(t *testing.T) {
		locationBlobPutWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(123)).Return(locationBlobPutWriter.Call, nil)
		locationBlobPutFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		locationBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
			func(b buffer.Buffer) local.LocationBlobPutFinalizer {
				data, err := b.ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return locationBlobPutFinalizer.Call
			})
		locationBlobPutFinalizer.EXPECT().Call().Return(location, nil)
		keyLocationMap.EXPECT().Put(key, location).
			Return(status.Error(codes.Internal, "Failed to insert entry"))

		keyBlobPutWriter, err := keyBlobMap.Put(123)
		require.NoError(t, err)
		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to insert entry"),
			keyBlobPutWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))(key))
	})

	t.Run("PutSuccess", func(t *testing.T) {
		locationBlobPutWriter := mock.NewMockLocationBlobPutWriter(ctrl)
		locationBlobMap.EXPECT().Put(int64(123)).Return(locationBlobPutWriter.Call, nil)
		locationBlobPutFinalizer := mock.NewMockLocationBlobPutFinalizer(ctrl)
		locationBlobPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
			func(b buffer.Buffer) local.LocationBlobPutFinalizer {
				data, err := b.ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return locationBlobPutFinalizer.Call
			})
		locationBlobPutFinalizer.EXPECT().Call().Return(location, nil)
		keyLocationMap.EXPECT().Put(key, location)

		keyBlobPutWriter, err := keyBlobMap.Put(123)
		require.NoError(t, err)
		require.NoError(
			t,
			keyBlobPutWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))(key))
	})
}
