package mirrored_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestResilientMirroredBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	replicatorAToB := mock.NewMockBlobReplicator(ctrl)
	replicatorBToA := mock.NewMockBlobReplicator(ctrl)
	blobDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	blobAccess := mirrored.NewResilientMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)

	t.Run("SuccessBackendAFails", func(t *testing.T) {
		// Backend A fails, but Backend B succeeds - operation should succeed
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("SuccessBackendBFails", func(t *testing.T) {
		// Backend B fails, but Backend A succeeds - operation should succeed
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("FailureBothBackendsFail", func(t *testing.T) {
		// Both backends fail - operation should fail
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Backend A on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Backend B on fire")
			})

		err := blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		require.Error(t, err)
		require.Contains(t, err.Error(), "Both backends failed")
	})
}

func TestResilientMirroredBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	replicatorAToB := mock.NewMockBlobReplicator(ctrl)
	replicatorBToA := mock.NewMockBlobReplicator(ctrl)
	digestA := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	digestB := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2", 9)
	allDigests := digest.NewSetBuilder().Add(digestA).Add(digestB).Build()
	missingFromA := digestA.ToSingletonSet()
	missingFromB := digestB.ToSingletonSet()
	blobAccess := mirrored.NewResilientMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)

	t.Run("SuccessBackendAFails", func(t *testing.T) {
		// Backend A fails, use results from Backend B only
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, missingFromB, missing)
	})

	t.Run("SuccessBackendBFails", func(t *testing.T) {
		// Backend B fails, use results from Backend A only
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, missingFromA, missing)
	})

	t.Run("FailureBothBackendsFail", func(t *testing.T) {
		// Both backends fail - operation should fail
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Backend A on fire"))
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Backend B on fire"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Both backends failed")
	})

	t.Run("SuccessWithReplicationFailure", func(t *testing.T) {
		// Both backends succeed but replication fails - operation should still succeed
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		// Replication fails but that's okay in resilient mode
		replicatorAToB.EXPECT().ReplicateMultiple(gomock.Any(), missingFromB).Return(status.Error(codes.Internal, "Replication failed"))
		replicatorBToA.EXPECT().ReplicateMultiple(gomock.Any(), missingFromA).Return(status.Error(codes.Internal, "Replication failed"))

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing) // No blobs are missing from both
	})
}

func TestResilientMirroredBlobAccessGetCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	replicatorAToB := mock.NewMockBlobReplicator(ctrl)
	replicatorBToA := mock.NewMockBlobReplicator(ctrl)
	instanceName := digest.MustNewInstanceName("test")
	capabilities := &remoteexecution.ServerCapabilities{
		ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{},
	}
	blobAccess := mirrored.NewResilientMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)

	t.Run("SuccessFirstBackendFails", func(t *testing.T) {
		// First backend (alternates) fails, second succeeds
		backendA.EXPECT().GetCapabilities(ctx, instanceName).Return(nil, status.Error(codes.Internal, "Server on fire"))
		backendB.EXPECT().GetCapabilities(ctx, instanceName).Return(capabilities, nil)

		result, err := blobAccess.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		require.Equal(t, capabilities, result)
	})

	t.Run("FailureBothBackendsFail", func(t *testing.T) {
		// Both backends fail - operation should fail
		backendB.EXPECT().GetCapabilities(ctx, instanceName).Return(nil, status.Error(codes.Internal, "Backend B on fire"))
		backendA.EXPECT().GetCapabilities(ctx, instanceName).Return(nil, status.Error(codes.Internal, "Backend A on fire"))

		_, err := blobAccess.GetCapabilities(ctx, instanceName)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Both backends failed")
	})
}
