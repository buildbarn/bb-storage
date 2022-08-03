package blobstore_test

import (
	"io"
	"testing"
	"time"

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

func TestValidationCachingReadBufferFactoryNewBufferFromByteSlice(t *testing.T) {
	ctrl := gomock.NewController(t)

	clock := mock.NewMockClock(ctrl)
	baseReadBufferFactory := mock.NewMockReadBufferFactory(ctrl)
	readBufferFactory := blobstore.NewValidationCachingReadBufferFactory(
		baseReadBufferFactory,
		digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet()))
	helloDigest := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)

	// In the initial state, blobs are assumed to not be validated.
	// All calls should be forwarded to the base ReadBufferFactory,
	// thereby causing integrity checking to be performed.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	baseReadBufferFactory.EXPECT().NewBufferFromByteSlice(
		helloDigest,
		[]byte("xyzzy"),
		gomock.Any(),
	).DoAndReturn(func(blobDigest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
		return buffer.NewCASBufferFromByteSlice(blobDigest, data, buffer.BackendProvided(dataIntegrityCallback))
	})
	dataIntegrityCallback1 := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback1.EXPECT().Call(false)

	_, err := readBufferFactory.NewBufferFromByteSlice(
		helloDigest,
		[]byte("xyzzy"),
		dataIntegrityCallback1.Call).ToByteSlice(10)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 1271ed5ef305aadabc605b1609e24c52, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)

	// The previous checksum failure should not cause data integrity
	// to be cached. A second call should also call into the base
	// ReadBufferFactory. This time, data is intact, thereby
	// creating a cache entry.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	baseReadBufferFactory.EXPECT().NewBufferFromByteSlice(
		helloDigest,
		[]byte("Hello"),
		gomock.Any(),
	).DoAndReturn(func(blobDigest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
		return buffer.NewCASBufferFromByteSlice(blobDigest, data, buffer.BackendProvided(dataIntegrityCallback))
	})
	dataIntegrityCallback2 := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback2.EXPECT().Call(true)

	data, err := readBufferFactory.NewBufferFromByteSlice(
		helloDigest,
		[]byte("Hello"),
		dataIntegrityCallback2.Call).ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)

	// During the next minute, calls for the same blob should cause
	// a Buffer to be created that doesn't to any data integrity
	// checking. The DataIntegrityCallback should be ignored
	// entirely, as nothing is known about the integrity of the
	// data.
	clock.EXPECT().Now().Return(time.Unix(1061, 0))
	dataIntegrityCallback3 := mock.NewMockDataIntegrityCallback(ctrl)

	data, err = readBufferFactory.NewBufferFromByteSlice(
		helloDigest,
		[]byte("xyzzy"),
		dataIntegrityCallback3.Call).ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("xyzzy"), data)

	// Once the cached entry has expired, data integrity checking
	// should be performed once again.
	clock.EXPECT().Now().Return(time.Unix(1063, 0))
	baseReadBufferFactory.EXPECT().NewBufferFromByteSlice(
		helloDigest,
		[]byte("xyzzy"),
		gomock.Any(),
	).DoAndReturn(func(blobDigest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
		return buffer.NewCASBufferFromByteSlice(blobDigest, data, buffer.BackendProvided(dataIntegrityCallback))
	})
	dataIntegrityCallback4 := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback4.EXPECT().Call(false)

	_, err = readBufferFactory.NewBufferFromByteSlice(
		helloDigest,
		[]byte("xyzzy"),
		dataIntegrityCallback4.Call).ToByteSlice(10)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 1271ed5ef305aadabc605b1609e24c52, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)
}

func TestValidationCachingReadBufferFactoryNewBufferFromReaderAt(t *testing.T) {
	// Reduced test case for NewBufferFromReaderAt() that is based
	// on the test above. We assume that all other cases are covered
	// sufficiently.
	ctrl := gomock.NewController(t)

	clock := mock.NewMockClock(ctrl)
	baseReadBufferFactory := mock.NewMockReadBufferFactory(ctrl)
	readBufferFactory := blobstore.NewValidationCachingReadBufferFactory(
		baseReadBufferFactory,
		digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet()))
	helloDigest := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)

	// In the initial state, blobs are assumed to not be validated.
	// All calls should be forwarded to the base ReadBufferFactory,
	// thereby causing integrity checking to be performed.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	fileReader1 := mock.NewMockReadAtCloser(ctrl)
	baseReadBufferFactory.EXPECT().NewBufferFromReaderAt(
		helloDigest,
		fileReader1,
		int64(5),
		gomock.Any(),
	).DoAndReturn(func(blobDigest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
		return buffer.NewCASBufferFromByteSlice(blobDigest, []byte("Hello"), buffer.BackendProvided(dataIntegrityCallback))
	})
	dataIntegrityCallback1 := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback1.EXPECT().Call(true)

	data, err := readBufferFactory.NewBufferFromReaderAt(
		helloDigest,
		fileReader1,
		5,
		dataIntegrityCallback1.Call).ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)

	// During the next minute, calls for the same blob should cause
	// a Buffer to be created that doesn't to any data integrity
	// checking. The DataIntegrityCallback should be ignored
	// entirely, as nothing is known about the integrity of the
	// data.
	clock.EXPECT().Now().Return(time.Unix(1061, 0))
	fileReader2 := mock.NewMockReadAtCloser(ctrl)
	fileReader2.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
		copy(p, "xyzzy")
		return 5, io.EOF
	})
	fileReader2.EXPECT().Close()
	dataIntegrityCallback2 := mock.NewMockDataIntegrityCallback(ctrl)

	data, err = readBufferFactory.NewBufferFromReaderAt(
		helloDigest,
		fileReader2,
		5,
		dataIntegrityCallback2.Call).ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("xyzzy"), data)
}
