package blobstore_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/mock"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMerkleBlobAccessSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	testSuccess := func(digest *util.Digest, body []byte) {
		// All calls are expect to go to the backend.
		bottomBlobAccess := mock.NewMockBlobAccess(ctrl)
		bottomBlobAccess.EXPECT().Get(
			ctx, digest,
		).Return(int64(len(body)), ioutil.NopCloser(bytes.NewBuffer(body)), nil)
		bottomBlobAccess.EXPECT().Put(
			ctx, digest, int64(len(body)), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
			buf, err := ioutil.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, body, buf)
			require.NoError(t, r.Close())
			return nil
		})
		bottomBlobAccess.EXPECT().Delete(ctx, digest).Return(nil)
		bottomBlobAccess.EXPECT().FindMissing(
			ctx, []*util.Digest{digest},
		).Return([]*util.Digest{digest}, nil)

		blobAccess := blobstore.NewMerkleBlobAccess(bottomBlobAccess)

		length, r, err := blobAccess.Get(ctx, digest)
		require.NoError(t, err)
		require.Equal(t, int64(len(body)), length)
		buf, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, body, buf)
		require.NoError(t, r.Close())

		require.NoError(t, blobAccess.Put(
			ctx, digest, digest.GetSizeBytes(),
			ioutil.NopCloser(bytes.NewBuffer(body))))

		require.NoError(t, blobAccess.Delete(ctx, digest))

		missing, err := blobAccess.FindMissing(ctx, []*util.Digest{digest})
		require.NoError(t, err)
		require.Equal(t, []*util.Digest{digest}, missing)
	}
	testSuccess(util.MustNewDigest("fedora29", &remoteexecution.Digest{
		Hash:      "8b1a9953c4611296a827abf8c47804d7",
		SizeBytes: 5,
	}), []byte("Hello"))
	testSuccess(util.MustNewDigest("windows10", &remoteexecution.Digest{
		Hash:      "a54d88e06612d820bc3be72877c74f257b561b19",
		SizeBytes: 14,
	}), []byte("This is a test"))
	testSuccess(util.MustNewDigest("solaris11", &remoteexecution.Digest{
		Hash:      "1d1f71aecd9b2d8127e5a91fc871833fffe58c5c63aceed9f6fd0b71fe732504",
		SizeBytes: 16,
	}), []byte("And another test"))
}

func TestMerkleBlobAccessMalformedData(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	testBadData := func(digest *util.Digest, bodyLength int64, body []byte, errorMessage string) {
		bottomBlobAccess := mock.NewMockBlobAccess(ctrl)

		// A Get() call yielding corrupted data should also
		// trigger a Delete() call on the storage backend, so
		// that inconsistencies are automatically repaired.
		bottomBlobAccess.EXPECT().Get(
			ctx, digest,
		).Return(bodyLength, ioutil.NopCloser(bytes.NewBuffer(body)), nil)
		bottomBlobAccess.EXPECT().Delete(ctx, digest).Return(nil)

		// A Put() call for uploading broken data does not
		// trigger a Delete(). If broken data ends up being
		// stored, future Get() calls will repair it for us.
		if bodyLength == digest.GetSizeBytes() {
			bottomBlobAccess.EXPECT().Put(
				ctx, digest, bodyLength, gomock.Any(),
			).DoAndReturn(func(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
				_, err := ioutil.ReadAll(r)
				s := status.Convert(err)
				require.Equal(t, codes.InvalidArgument, s.Code())
				require.Equal(t, errorMessage, s.Message())
				require.NoError(t, r.Close())
				return err
			})
		}

		blobAccess := blobstore.NewMerkleBlobAccess(bottomBlobAccess)

		// A Get() call on corrupt data should trigger an
		// internal error on the server.
		_, r, err := blobAccess.Get(ctx, digest)
		if err == nil {
			require.NoError(t, err)
			_, err = ioutil.ReadAll(r)
			require.NoError(t, r.Close())
		}
		s := status.Convert(err)
		require.Equal(t, codes.Internal, s.Code())
		require.Equal(t, errorMessage, s.Message())

		// A Put() call for corrupt data should return an
		// invalid argument error instead.
		err = blobAccess.Put(
			ctx, digest, bodyLength,
			ioutil.NopCloser(bytes.NewBuffer(body)))
		s = status.Convert(err)
		require.Equal(t, codes.InvalidArgument, s.Code())
		require.Equal(t, errorMessage, s.Message())
	}
	testBadData(
		util.MustNewDigest(
			"freebsd12",
			&remoteexecution.Digest{
				Hash:      "3e25960a79dbc69b674cd4ec67a72c62",
				SizeBytes: 11,
			}),
		5, []byte("Hello"),
		"Blob is 5 bytes in size, while 11 bytes were expected")
	testBadData(
		util.MustNewDigest(
			"freebsd12",
			&remoteexecution.Digest{
				Hash:      "3e25960a79dbc69b674cd4ec67a72c62",
				SizeBytes: 11,
			}),
		11, []byte("Hello"),
		"Blob is 6 bytes shorter than expected")
	testBadData(
		util.MustNewDigest(
			"windows10",
			&remoteexecution.Digest{
				Hash:      "8b1a9953c4611296a827abf8c47804d7",
				SizeBytes: 5,
			}),
		5, []byte("Hello world"),
		"Blob is longer than expected")
	testBadData(
		util.MustNewDigest(
			"ubuntu1804",
			&remoteexecution.Digest{
				Hash:      "8b1a9953c4611296a827abf8c47804d7",
				SizeBytes: 11,
			}),
		11, []byte("Hello world"),
		"Checksum of blob is 3e25960a79dbc69b674cd4ec67a72c62, "+
			"while 8b1a9953c4611296a827abf8c47804d7 was expected")
	testBadData(
		util.MustNewDigest(
			"archlinux",
			&remoteexecution.Digest{
				Hash:      "f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0",
				SizeBytes: 11,
			}),
		11, []byte("Hello world"),
		"Checksum of blob is 7b502c3a1f48c8609ae212cdfb639dee39673f5e, "+
			"while f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0 was expected")
	testBadData(
		util.MustNewDigest(
			"macos",
			&remoteexecution.Digest{
				Hash:      "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969",
				SizeBytes: 11,
			}),
		11, []byte("Hello world"),
		"Checksum of blob is 64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c, "+
			"while 185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969 was expected")
}
