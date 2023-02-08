package blobstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReferenceExpandingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)
	s3Client := mock.NewMockS3Client(ctrl)
	gcsClient := mock.NewMockStorageClient(ctrl)
	blobAccess := blobstore.NewReferenceExpandingBlobAccess(
		baseBlobAccess,
		&http.Client{Transport: roundTripper},
		s3Client,
		gcsClient,
		100)
	helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("BackendError", func(t *testing.T) {
		// The ICAS backend returning an error.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to load reference: I/O error"), err)
	})

	t.Run("InvalidReference", func(t *testing.T) {
		// The ICAS returning an entry that does not contain a
		// reference for a supported medium.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "Reference uses an unsupported medium"), err)
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
					OffsetBytes: 100,
					SizeBytes:   5,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to create HTTP request: parse \"\\x00\": net/url: invalid control character in URL"), err)
	})

	t.Run("HTTPRequestFailed", func(t *testing.T) {
		// The HTTP server returns no valid HTTP response.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file.txt",
					},
					OffsetBytes: 100,
					SizeBytes:   5,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(nil, errors.New("dial tcp 1.2.3.4:80: connect: connection refused"))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "HTTP request failed: Get \"http://example.com/file.txt\": dial tcp 1.2.3.4:80: connect: connection refused"), err)
	})

	t.Run("HTTPBadStatusCode", func(t *testing.T) {
		// The HTTP server returns a response other than
		// 206 Partial Content.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file.txt",
					},
					OffsetBytes: 100,
					SizeBytes:   5,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		body := mock.NewMockReadCloser(ctrl)
		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:     "404 Not Found",
			StatusCode: 404,
			Body:       body,
		}, nil)
		body.EXPECT().Close()

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "HTTP request failed with status \"404 Not Found\""), err)
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
					OffsetBytes: 100,
					SizeBytes:   5,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		body := mock.NewMockReadCloser(ctrl)
		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:     "206 Partial Content",
			StatusCode: 206,
			Body:       body,
		}, nil)
		body.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			copy(p, "Hallo")
			return 5, io.EOF
		})
		body.EXPECT().Close()

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum d1bf93299de1b68e6d382c893bf1215f, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)
	})

	t.Run("HTTPSuccessPlain", func(t *testing.T) {
		// The HTTP server returns valid data.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file.txt",
					},
					OffsetBytes: 100,
					SizeBytes:   5,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		body := mock.NewMockReadCloser(ctrl)
		roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(
			func(req *http.Request) (*http.Response, error) {
				require.Equal(t, "GET", req.Method)
				require.Equal(t, "http://example.com/file.txt", req.URL.String())
				require.Equal(t, "bytes=100-104", req.Header.Get("Range"))
				return &http.Response{
					Status:     "206 Partial Content",
					StatusCode: 206,
					Body:       body,
				}, nil
			})
		body.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			copy(p, "Hello")
			return 5, io.EOF
		})
		body.EXPECT().Close()

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("S3RequestFailed", func(t *testing.T) {
		// The S3 service returns an error.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_S3_{
						S3: &icas.Reference_S3{
							Bucket: "mybucket",
							Key:    "mykey",
						},
					},
					OffsetBytes:  100,
					SizeBytes:    11,
					Decompressor: remoteexecution.Compressor_DEFLATE,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		s3Client.EXPECT().GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("mybucket"),
			Key:    aws.String("mykey"),
			Range:  aws.String("bytes=100-110"),
		}).Return(nil, &types.NoSuchKey{
			Message: aws.String("The specified key does not exist. status code: 404, request id: ..., host id: ..."),
		})

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "S3 request failed: NoSuchKey: The specified key does not exist. status code: 404, request id: ..., host id: ..."), err)
	})

	t.Run("S3DeflateError", func(t *testing.T) {
		// The data returned by S3 cannot be decompressed.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_S3_{
						S3: &icas.Reference_S3{
							Bucket: "mybucket",
							Key:    "mykey",
						},
					},
					OffsetBytes:  100,
					SizeBytes:    11,
					Decompressor: remoteexecution.Compressor_DEFLATE,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		body := mock.NewMockReadCloser(ctrl)
		s3Client.EXPECT().GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("mybucket"),
			Key:    aws.String("mykey"),
			Range:  aws.String("bytes=100-110"),
		}).Return(&s3.GetObjectOutput{
			Body: body,
		}, nil)
		body.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			copy(p, []byte{0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07})
			return 6, io.EOF
		})
		body.EXPECT().Close()

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, io.ErrUnexpectedEOF, err)
	})

	t.Run("S3SuccessDeflate", func(t *testing.T) {
		// The S3 service returns valid compressed data.
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_S3_{
						S3: &icas.Reference_S3{
							Bucket: "mybucket",
							Key:    "mykey",
						},
					},
					OffsetBytes:  100,
					Decompressor: remoteexecution.Compressor_DEFLATE,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		body := mock.NewMockReadCloser(ctrl)
		s3Client.EXPECT().GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("mybucket"),
			Key:    aws.String("mykey"),
			Range:  aws.String("bytes=100-"),
		}).Return(&s3.GetObjectOutput{
			Body: body,
		}, nil)
		body.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			// The word "Hello" compressed with DEFLATE.
			copy(p, []byte{0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x04, 0x00, 0x00, 0xff, 0xff})
			return 11, io.EOF
		})
		body.EXPECT().Close()

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("S3SuccessZstandard", func(t *testing.T) {
		// The S3 service returns valid data compressed using
		// the Zstandard algorithm.
		aaaDigest := digest.MustNewDigest("foo", remoteexecution.DigestFunction_SHA256, "160b4e433e384e05e537dc59b467f7cb2403f0214db15c5db58862a3f1156d2e", 50)
		baseBlobAccess.EXPECT().Get(ctx, aaaDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_S3_{
						S3: &icas.Reference_S3{
							Bucket: "mybucket",
							Key:    "mykey",
						},
					},
					OffsetBytes:  0,
					SizeBytes:    21,
					Decompressor: remoteexecution.Compressor_ZSTD,
				},
				buffer.BackendProvided(buffer.Irreparable(aaaDigest))))
		body := mock.NewMockReadCloser(ctrl)
		s3Client.EXPECT().GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("mybucket"),
			Key:    aws.String("mykey"),
			Range:  aws.String("bytes=0-20"),
		}).Return(&s3.GetObjectOutput{
			Body: body,
		}, nil)
		body.EXPECT().Read(gomock.Any()).
			DoAndReturn(bytes.NewBuffer([]byte{
				// 21 bytes of Zstandard compressed data
				// that decompress to fifty 'a' bytes.
				0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x45,
				0x00, 0x00, 0x10, 0x61, 0x61, 0x01, 0x00,
				0x45, 0x00, 0x0b, 0x23, 0x9f, 0x0f, 0x9a,
			}).Read).
			AnyTimes()
		body.EXPECT().Close()

		data, err := blobAccess.Get(ctx, aaaDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), data)
	})

	t.Run("GCSSuccess", func(t *testing.T) {
		helloDigest := digest.MustNewDigest("foo", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_Gcs{
						Gcs: &icas.Reference_GCS{
							Bucket: "mybucket",
							Object: "myobject",
						},
					},
					OffsetBytes:  3,
					SizeBytes:    0,
					Decompressor: remoteexecution.Compressor_IDENTITY,
				},
				buffer.BackendProvided(buffer.Irreparable(helloDigest))))
		bucketHandle := mock.NewMockStorageBucketHandle(ctrl)
		gcsClient.EXPECT().Bucket("mybucket").Return(bucketHandle)
		objectHandle := mock.NewMockStorageObjectHandle(ctrl)
		bucketHandle.EXPECT().Object("myobject").Return(objectHandle)
		body := mock.NewMockReadCloser(ctrl)
		objectHandle.EXPECT().NewRangeReader(ctx, int64(3), int64(-1)).Return(body, nil)
		body.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			copy(p, "Hello")
			return 5, io.EOF
		})
		body.EXPECT().Close()

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestReferenceExpandingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)
	s3Client := mock.NewMockS3Client(ctrl)
	gcsClient := mock.NewMockStorageClient(ctrl)
	blobAccess := blobstore.NewReferenceExpandingBlobAccess(
		baseBlobAccess,
		&http.Client{Transport: roundTripper},
		s3Client,
		gcsClient,
		100)

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
					remoteexecution.DigestFunction_MD5,
					"8b1a9953c4611296a827abf8c47804d7",
					5),
				buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestReferenceExpandingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)
	s3Client := mock.NewMockS3Client(ctrl)
	gcsClient := mock.NewMockStorageClient(ctrl)
	blobAccess := blobstore.NewReferenceExpandingBlobAccess(
		baseBlobAccess,
		&http.Client{Transport: roundTripper},
		s3Client,
		gcsClient,
		100)

	digests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
		Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "f5a7924e621e84c9280a9a27e1bcb7f6", 5)).
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
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Network error"), err)
	})
}
