package grpcservers_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIndirectContentAddressableStorageServerFindMissingReferences(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	s := grpcservers.NewIndirectContentAddressableStorageServer(blobAccess, 1000)

	t.Run("BadDigest", func(t *testing.T) {
		// Malformed requests cannot be executed.
		_, err := s.FindMissingReferences(ctx, &remoteexecution.FindMissingBlobsRequest{
			InstanceName: "example",
			BlobDigests: []*remoteexecution.Digest{
				{
					Hash:      "This is not a valid hash",
					SizeBytes: 123,
				},
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown digest hash length: 24 characters"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Errors returned by the backend should be forwarded.
		blobAccess.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("example", "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build()).
			Return(digest.EmptySet, status.Error(codes.Internal, "Hardware failure"))

		_, err := s.FindMissingReferences(ctx, &remoteexecution.FindMissingBlobsRequest{
			InstanceName: "example",
			BlobDigests: []*remoteexecution.Digest{
				{
					Hash:      "8b1a9953c4611296a827abf8c47804d7",
					SizeBytes: 5,
				},
				{
					Hash:      "6fc422233a40a75a1f028e11c3cd1140",
					SizeBytes: 7,
				},
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Hardware failure"), err)
	})

	t.Run("Success", func(t *testing.T) {
		blobAccess.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("example", "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build()).
			Return(digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet(), nil)

		resp, err := s.FindMissingReferences(ctx, &remoteexecution.FindMissingBlobsRequest{
			InstanceName: "example",
			BlobDigests: []*remoteexecution.Digest{
				{
					Hash:      "8b1a9953c4611296a827abf8c47804d7",
					SizeBytes: 5,
				},
				{
					Hash:      "6fc422233a40a75a1f028e11c3cd1140",
					SizeBytes: 7,
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &remoteexecution.FindMissingBlobsResponse{
			MissingBlobDigests: []*remoteexecution.Digest{
				{
					Hash:      "8b1a9953c4611296a827abf8c47804d7",
					SizeBytes: 5,
				},
			},
		}, resp)
	})
}

func TestIndirectContentAddressableStorageServerBatchUpdateReferences(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	s := grpcservers.NewIndirectContentAddressableStorageServer(blobAccess, 1000)

	t.Run("Mixed", func(t *testing.T) {
		// Send a single batch update request containing three
		// entries: one with an invalid digest, one that doesn't
		// exist, one that triggers an I/O error, and one that
		// can be written successfully.
		blobAccess.EXPECT().Put(
			ctx,
			digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5),
			gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Disk I/O failure")
			})
		blobAccess.EXPECT().Put(
			ctx,
			digest.MustNewDigest("example", "6fc422233a40a75a1f028e11c3cd1140", 7),
			gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&icas.Reference{}, 1000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file3.txt",
					},
				}, m)
				return nil
			})

		resp, err := s.BatchUpdateReferences(ctx, &icas.BatchUpdateReferencesRequest{
			InstanceName: "example",
			Requests: []*icas.BatchUpdateReferencesRequest_Request{
				{
					Digest: &remoteexecution.Digest{
						Hash:      "This is not a valid hash",
						SizeBytes: 123,
					},
					Reference: &icas.Reference{
						Medium: &icas.Reference_HttpUrl{
							HttpUrl: "http://example.com/file1.txt",
						},
					},
				},
				{
					Digest: &remoteexecution.Digest{
						Hash:      "8b1a9953c4611296a827abf8c47804d7",
						SizeBytes: 5,
					},
					Reference: &icas.Reference{
						Medium: &icas.Reference_HttpUrl{
							HttpUrl: "http://example.com/file2.txt",
						},
					},
				},
				{
					Digest: &remoteexecution.Digest{
						Hash:      "6fc422233a40a75a1f028e11c3cd1140",
						SizeBytes: 7,
					},
					Reference: &icas.Reference{
						Medium: &icas.Reference_HttpUrl{
							HttpUrl: "http://example.com/file3.txt",
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &remoteexecution.BatchUpdateBlobsResponse{
			Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{
					Digest: &remoteexecution.Digest{
						Hash:      "This is not a valid hash",
						SizeBytes: 123,
					},
					Status: status.New(codes.InvalidArgument, "Unknown digest hash length: 24 characters").Proto(),
				},
				{
					Digest: &remoteexecution.Digest{
						Hash:      "8b1a9953c4611296a827abf8c47804d7",
						SizeBytes: 5,
					},
					Status: status.New(codes.Internal, "Disk I/O failure").Proto(),
				},
				{
					Digest: &remoteexecution.Digest{
						Hash:      "6fc422233a40a75a1f028e11c3cd1140",
						SizeBytes: 7,
					},
				},
			},
		}, resp)
	})
}

func TestIndirectContentAddressableStorageServerGetReference(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	s := grpcservers.NewIndirectContentAddressableStorageServer(blobAccess, 1000)

	t.Run("BadDigest", func(t *testing.T) {
		// Malformed requests cannot be executed.
		_, err := s.GetReference(ctx, &icas.GetReferenceRequest{
			InstanceName: "example",
			Digest: &remoteexecution.Digest{
				Hash:      "This is not a valid hash",
				SizeBytes: 123,
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown digest hash length: 24 characters"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Errors returned by the backend should be forwarded.
		blobAccess.EXPECT().Get(
			ctx,
			digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Hardware failure")))

		_, err := s.GetReference(ctx, &icas.GetReferenceRequest{
			InstanceName: "example",
			Digest: &remoteexecution.Digest{
				Hash:      "8b1a9953c4611296a827abf8c47804d7",
				SizeBytes: 5,
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Hardware failure"), err)
	})

	t.Run("Success", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)
		blobAccess.EXPECT().Get(
			ctx,
			digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)).
			Return(buffer.NewProtoBufferFromProto(
				&icas.Reference{
					Medium: &icas.Reference_HttpUrl{
						HttpUrl: "http://example.com/file3.txt",
					},
				},
				buffer.BackendProvided(dataIntegrityCallback.Call)))

		resp, err := s.GetReference(ctx, &icas.GetReferenceRequest{
			InstanceName: "example",
			Digest: &remoteexecution.Digest{
				Hash:      "8b1a9953c4611296a827abf8c47804d7",
				SizeBytes: 5,
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &icas.Reference{
			Medium: &icas.Reference_HttpUrl{
				HttpUrl: "http://example.com/file3.txt",
			},
		}, resp)
	})
}
