package blobstore_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReferenceExpandingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	httpClient := mock.NewMockHTTPClient(ctrl)
	blobAccess := blobstore.NewReferenceExpandingBlobAccess(baseBlobAccess, httpClient, 100)
	helloDigest := digest.MustNewDigest("instance", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("BackendError", func(t *testing.T) {
		// The ICAS backend returning an error.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Failed to load reference: I/O error"), err)
	})

	t.Run("InvalidReference", func(t *testing.T) {
		// The ICAS returning an entry that does not contain a
		// reference for a supported medium.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{},
				buffer.Irreparable))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Unimplemented, "Reference uses an unsupported medium"), err)
	})

	t.Run("HTTPInvalidURL", func(t *testing.T) {
		// The ICAS returning an entry with a malformed URL,
		// which prevents us from creating a HTTP request.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "\x00",
					},
				},
				buffer.Irreparable))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Failed to create HTTP request: parse \"\\x00\": net/url: invalid control character in URL"), err)
	})

	t.Run("HTTPRequestFailed", func(t *testing.T) {
		// The HTTP server returns no valid HTTP response.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file.txt",
					},
				},
				buffer.Irreparable))
		httpClient.EXPECT().Do(gomock.Any()).Return(nil, &url.Error{
			Op:  "Get",
			URL: "http://example.com/file.txt",
			Err: errors.New("dial tcp 1.2.3.4:80: connect: connection refused"),
		})

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "HTTP request failed: Get \"http://example.com/file.txt\": dial tcp 1.2.3.4:80: connect: connection refused"), err)
	})

	t.Run("HTTPBadStatusCode", func(t *testing.T) {
		// The HTTP server returns a response other than 200 OK.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file.txt",
					},
				},
				buffer.Irreparable))
		body := mock.NewMockReadCloser(ctrl)
		httpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			Status:     "404 Not Found",
			StatusCode: 404,
			Body:       body,
		}, nil)
		body.EXPECT().Close()

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "HTTP request failed with status \"404 Not Found\""), err)
	})

	t.Run("HTTPChecksumFailure", func(t *testing.T) {
		// The HTTP server returns data, but it does not
		// correspond with the digest of the object.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file.txt",
					},
				},
				buffer.Irreparable))
		body := mock.NewMockReadCloser(ctrl)
		httpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Body:       body,
		}, nil)
		body.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			copy(p, "Hallo")
			return 5, io.EOF
		})
		body.EXPECT().Close()

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Buffer has checksum d1bf93299de1b68e6d382c893bf1215f, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)
	})

	t.Run("HTTPSuccess", func(t *testing.T) {
		// The HTTP server returns valid data.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file.txt",
					},
				},
				buffer.Irreparable))
		body := mock.NewMockReadCloser(ctrl)
		httpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Body:       body,
		}, nil)
		body.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			copy(p, "Hello")
			return 5, io.EOF
		})
		body.EXPECT().Close()

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestReferenceExpandingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	httpClient := mock.NewMockHTTPClient(ctrl)
	blobAccess := blobstore.NewReferenceExpandingBlobAccess(baseBlobAccess, httpClient, 100)

	t.Run("Failure", func(t *testing.T) {
		// It is not possible to write objects using
		// ReferenceExpandingBlobAccess, as it wouldn't know
		// where to store the data.
		require.Equal(
			t,
			status.Error(codes.InvalidArgument, "The Indirect Content Addressable Storage can only store references, not data"),
			blobAccess.Put(
				ctx,
				digest.MustNewDigest(
					"instance",
					"8b1a9953c4611296a827abf8c47804d7",
					5),
				buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestReferenceExpandingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	httpClient := mock.NewMockHTTPClient(ctrl)
	blobAccess := blobstore.NewReferenceExpandingBlobAccess(baseBlobAccess, httpClient, 100)

	digests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", "8b1a9953c4611296a827abf8c47804d7", 5)).
		Add(digest.MustNewDigest("instance", "f5a7924e621e84c9280a9a27e1bcb7f6", 5)).
		Build()

	t.Run("Success", func(t *testing.T) {
		// Calls to FindMissing() should be forwarded literally.
		// The ICAS alone is used as an index to determine which
		// objects are available. No checks against the actual
		// storage backend holding the data are performed, as
		// that would be too costly.
		baseBlobAccess.EXPECT().FindMissing(ctx, digests).
			Return(digests, nil)

		missing, err := blobAccess.FindMissing(ctx, digests)
		require.NoError(t, err)
		require.Equal(t, digests, missing)
	})

	t.Run("Failure", func(t *testing.T) {
		baseBlobAccess.EXPECT().FindMissing(ctx, digests).
			Return(digest.EmptySet, status.Error(codes.Internal, "Network error"))

		_, err := blobAccess.FindMissing(ctx, digests)
		require.Equal(t, status.Error(codes.Internal, "Network error"), err)
	})
}
