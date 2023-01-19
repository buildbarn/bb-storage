package buffer_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
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

	for _, entry := range []struct {
		digestFunction remoteexecution.DigestFunction_Value
		hash           string
		body           []byte
	}{
		{remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", []byte("Hello")},
		{remoteexecution.DigestFunction_SHA1, "a54d88e06612d820bc3be72877c74f257b561b19", []byte("This is a test")},
		{remoteexecution.DigestFunction_SHA256, "1d1f71aecd9b2d8127e5a91fc871833fffe58c5c63aceed9f6fd0b71fe732504", []byte("And another test")},
		{remoteexecution.DigestFunction_SHA384, "8eb24e0851260f9ee83e88a47a0ae76871c8c8a8befdfc39931b42a334cd0fcd595e8e6766ef471e5f2d50b74e041e8d", []byte("Even longer checksums")},
		{remoteexecution.DigestFunction_SHA512, "b1d33bb21db304209f584b55e1a86db38c7c44c466c680c38805db07a92d43260d0e82ffd0a48c337d40372a4ac5b9be1ff24beef2c990e6ea3f2079d067b0e0", []byte("Ridiculously long checksums")},
		{remoteexecution.DigestFunction_SHA256TREE, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", []byte("Hello")},
	} {
		digest := digest.MustNewDigest("fedora29", entry.digestFunction, entry.hash, int64(len(entry.body)))
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)

		data, err := buffer.NewCASBufferFromByteSlice(
			digest,
			entry.body,
			buffer.BackendProvided(dataIntegrityCallback.Call)).ToByteSlice(len(entry.body))
		require.NoError(t, err)
		require.Equal(t, entry.body, data)
	}
}

func TestNewCASBufferFromByteSliceSizeMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 6)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(false)

	_, err := buffer.NewCASBufferFromByteSlice(
		digest,
		[]byte("Hello"),
		buffer.BackendProvided(dataIntegrityCallback.Call)).ToByteSlice(5)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer is 5 bytes in size, while 6 bytes were expected"), err)
}

func TestNewCASBufferFromByteSliceHashMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	digest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 5)
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(false)

	_, err := buffer.NewCASBufferFromByteSlice(
		digest,
		[]byte("Hello"),
		buffer.BackendProvided(dataIntegrityCallback.Call)).ToByteSlice(5)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 8b1a9953c4611296a827abf8c47804d7, while d41d8cd98f00b204e9800998ecf8427e was expected"), err)
}
