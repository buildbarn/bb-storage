package buffer_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These tests only cover aspects of NewCASBufferFromByteSlice() itself.
// Testing coverage for the actual behavior of the Buffer object is
// provided by TestNewValidatedBufferFromByteSlice*() and
// TestNewBufferFromError*().

func TestNewCASBufferFromByteSliceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	for hash, body := range map[string][]byte{
		// MD5:
		"8b1a9953c4611296a827abf8c47804d7": []byte("Hello"),
		// SHA-1:
		"a54d88e06612d820bc3be72877c74f257b561b19": []byte("This is a test"),
		// SHA-256:
		"1d1f71aecd9b2d8127e5a91fc871833fffe58c5c63aceed9f6fd0b71fe732504": []byte("And another test"),
		// SHA-384:
		"8eb24e0851260f9ee83e88a47a0ae76871c8c8a8befdfc39931b42a334cd0fcd595e8e6766ef471e5f2d50b74e041e8d": []byte("Even longer checksums"),
		// SHA-512:
		"b1d33bb21db304209f584b55e1a86db38c7c44c466c680c38805db07a92d43260d0e82ffd0a48c337d40372a4ac5b9be1ff24beef2c990e6ea3f2079d067b0e0": []byte("Ridiculously long checksums"),
	} {
		digest := digest.MustNewDigest("fedora29", hash, int64(len(body)))
		repairFunc := mock.NewMockRepairFunc(ctrl)

		data, err := buffer.NewCASBufferFromByteSlice(
			digest,
			body,
			buffer.Reparable(digest, repairFunc.Call)).ToByteSlice(len(body))
		require.NoError(t, err)
		require.Equal(t, body, data)
	}
}

func TestNewCASBufferFromByteSliceSizeMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("ubuntu1804", "8b1a9953c4611296a827abf8c47804d7", 6)
	repairFunc := mock.NewMockRepairFunc(ctrl)
	repairFunc.EXPECT().Call()

	_, err := buffer.NewCASBufferFromByteSlice(
		digest,
		[]byte("Hello"),
		buffer.Reparable(digest, repairFunc.Call)).ToByteSlice(5)
	require.Equal(t, status.Error(codes.Internal, "Buffer is 5 bytes in size, while 6 bytes were expected"), err)
}

func TestNewCASBufferFromByteSliceHashMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("ubuntu1804", "d41d8cd98f00b204e9800998ecf8427e", 5)
	repairFunc := mock.NewMockRepairFunc(ctrl)
	repairFunc.EXPECT().Call()

	_, err := buffer.NewCASBufferFromByteSlice(
		digest,
		[]byte("Hello"),
		buffer.Reparable(digest, repairFunc.Call)).ToByteSlice(5)
	require.Equal(t, status.Error(codes.Internal, "Buffer has checksum 8b1a9953c4611296a827abf8c47804d7, while d41d8cd98f00b204e9800998ecf8427e was expected"), err)
}
