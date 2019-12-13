package blobstore_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMirroredBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	digest := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		})

	t.Run("Success", func(t *testing.T) {
		// Requests should alternate between backends to spread
		// the load between backends equally.
		gomock.InOrder(
			backendA.EXPECT().Get(ctx, digest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))),
			backendB.EXPECT().Get(ctx, digest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))),
			backendA.EXPECT().Get(ctx, digest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))),
		)

		blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)
		for i := 0; i < 3; i++ {
			data, err := blobAccess.Get(ctx, digest).ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), data)
		}
	})

	t.Run("NotFoundBoth", func(t *testing.T) {
		// Simulate the case where a blob is not present in both
		// backends. It will try to synchronize the blob from
		// backend B to backend A, but this will fail.
		backendA.EXPECT().Get(ctx, digest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, digest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendA.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				_, err := b.ToByteSlice(100)
				require.Equal(t, status.Error(codes.NotFound, "Blob not found"), err)
				return err
			})

		blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)
		_, err := blobAccess.Get(ctx, digest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("RepairSuccess", func(t *testing.T) {
		// The blob is only present in the second backend. It
		// will get synchronized into the first.
		backendA.EXPECT().Get(ctx, digest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, digest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendA.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)
		data, err := blobAccess.Get(ctx, digest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("RepairError", func(t *testing.T) {
		// The blob is only present in the second backend. It
		// will get synchronized into the first.
		backendA.EXPECT().Get(ctx, digest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, digest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendA.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)
		_, err := blobAccess.Get(ctx, digest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Backend A: Server on fire"), err)
	})

	t.Run("ErrorBackendA", func(t *testing.T) {
		backendA.EXPECT().Get(ctx, digest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))

		// In case of fatal errors, the name of the backend
		// should be prepended.
		blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)
		_, err := blobAccess.Get(ctx, digest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Backend A: Server on fire"), err)
	})

	t.Run("ErrorBackendB", func(t *testing.T) {
		backendA.EXPECT().Get(ctx, digest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, digest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		backendA.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				_, err := b.ToByteSlice(100)
				require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
				return err
			})

		blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)
		_, err := blobAccess.Get(ctx, digest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Backend B: Server on fire"), err)
	})
}

func TestMirroredBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	digest := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		})
	blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)

	t.Run("Success", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		require.NoError(t, blobAccess.Put(ctx, digest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("ErrorBackendA", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				b.Discard()
				return nil
			})

		require.Equal(
			t,
			status.Error(codes.Internal, "Backend A: Server on fire"),
			blobAccess.Put(ctx, digest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("ErrorBackendB", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				b.Discard()
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), digest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		require.Equal(
			t,
			status.Error(codes.Internal, "Backend B: Server on fire"),
			blobAccess.Put(ctx, digest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})
}

func TestMirroredBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	digestNone := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		})
	digestA := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 0,
		})
	digestB := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2",
			SizeBytes: 9,
		})
	digestBoth := util.MustNewDigest(
		"default",
		&remoteexecution.Digest{
			Hash:      "9c6079651d4062b6811f93061cb6a768a60e51d714bddffee99b1173c6580580",
			SizeBytes: 5,
		})
	allDigests := []*util.Digest{digestNone, digestA, digestB, digestBoth}
	blobAccess := blobstore.NewMirroredBlobAccess(backendA, backendB)

	t.Run("Success", func(t *testing.T) {
		// Listings of both backends should be requested.
		backendA.EXPECT().FindMissing(ctx, allDigests).Return([]*util.Digest{digestNone, digestB}, nil)
		backendB.EXPECT().FindMissing(ctx, allDigests).Return([]*util.Digest{digestNone, digestA}, nil)

		// Blobs missing in one backend, but present in the
		// other should be exchanged.
		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice(nil))
		backendB.EXPECT().Put(ctx, digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Empty(t, data)
				return nil
			})
		backendB.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Buildbarn")))
		backendA.EXPECT().Put(ctx, digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Buildbarn"), data)
				return nil
			})

		// The intersection of missing blobs in the backends
		// should be returned.
		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, []*util.Digest{digestNone}, missing)
	})

	t.Run("FindMissingErrorBackendA", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, allDigests).Return(nil, status.Error(codes.Internal, "Server on fire"))
		backendB.EXPECT().FindMissing(ctx, allDigests).Return([]*util.Digest{digestNone, digestA}, nil)

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, status.Error(codes.Internal, "Backend A: Server on fire"), err)
	})

	t.Run("FindMissingErrorBackendB", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, allDigests).Return([]*util.Digest{digestNone, digestB}, nil)
		backendB.EXPECT().FindMissing(ctx, allDigests).Return(nil, status.Error(codes.Internal, "Server on fire"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, status.Error(codes.Internal, "Backend B: Server on fire"), err)
	})

	t.Run("PutErrorBackendA", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, []*util.Digest{digestB}).Return([]*util.Digest{digestB}, nil)
		backendB.EXPECT().FindMissing(ctx, []*util.Digest{digestB}).Return(nil, nil)

		backendB.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Buildbarn")))
		backendA.EXPECT().Put(ctx, digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		_, err := blobAccess.FindMissing(ctx, []*util.Digest{digestB})
		require.Equal(t, status.Error(codes.Internal, "Failed to synchronize blob 522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2-9-default from backend B to backend A: Server on fire"), err)
	})

	t.Run("PutErrorBackendB", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, []*util.Digest{digestA}).Return(nil, nil)
		backendB.EXPECT().FindMissing(ctx, []*util.Digest{digestA}).Return([]*util.Digest{digestA}, nil)

		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice(nil))
		backendB.EXPECT().Put(ctx, digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		_, err := blobAccess.FindMissing(ctx, []*util.Digest{digestA})
		require.Equal(t, status.Error(codes.Internal, "Failed to synchronize blob e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-0-default from backend A to backend B: Server on fire"), err)
	})
}
