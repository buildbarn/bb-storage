package blobstore_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestDemultiplexingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	demultiplexedBlobAccessGetter := mock.NewMockDemultiplexedBlobAccessGetter(ctrl)
	blobAccess := blobstore.NewDemultiplexingBlobAccess(demultiplexedBlobAccessGetter.Call)

	t.Run("UnknownInstanceName", func(t *testing.T) {
		// This request cannot be forwarded to any backend.
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("unknown")).Return(
			nil,
			"",
			digest.NoopInstanceNamePatcher,
			status.Error(codes.InvalidArgument, "Unknown instance name"))

		_, err := blobAccess.Get(
			ctx,
			digest.MustNewDigest("unknown", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown instance name"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Error messages should have the backend name prepended.
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		baseBlobAccess.EXPECT().Get(ctx, digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))

		_, err := blobAccess.Get(
			ctx,
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend \"Primary\": Server on fire"), err)
	})

	t.Run("Success", func(t *testing.T) {
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		baseBlobAccess.EXPECT().Get(ctx, digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(
			ctx,
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestDemultiplexingBlobAccessGetFromComposite(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	demultiplexedBlobAccessGetter := mock.NewMockDemultiplexedBlobAccessGetter(ctrl)
	blobAccess := blobstore.NewDemultiplexingBlobAccess(demultiplexedBlobAccessGetter.Call)

	t.Run("UnknownInstanceName", func(t *testing.T) {
		// This request cannot be forwarded to any backend.
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("unknown")).Return(
			nil,
			"",
			digest.NoopInstanceNamePatcher,
			status.Error(codes.InvalidArgument, "Unknown instance name"))
		blobSlicer := mock.NewMockBlobSlicer(ctrl)

		_, err := blobAccess.GetFromComposite(
			ctx,
			digest.MustNewDigest("unknown", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
			digest.MustNewDigest("unknown", remoteexecution.DigestFunction_MD5, "3123059c1c816471780539f6b6b738dc", 3),
			blobSlicer,
		).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown instance name"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Error messages should have the backend name prepended.
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		blobSlicer := mock.NewMockBlobSlicer(ctrl)
		baseBlobAccess.EXPECT().GetFromComposite(
			ctx,
			digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
			digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "3123059c1c816471780539f6b6b738dc", 3),
			blobSlicer,
		).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))

		_, err := blobAccess.GetFromComposite(
			ctx,
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "3123059c1c816471780539f6b6b738dc", 3),
			blobSlicer,
		).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend \"Primary\": Server on fire"), err)
	})

	t.Run("Success", func(t *testing.T) {
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		blobSlicer := mock.NewMockBlobSlicer(ctrl)
		baseBlobAccess.EXPECT().GetFromComposite(
			ctx,
			digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
			digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "3123059c1c816471780539f6b6b738dc", 3),
			blobSlicer,
		).Return(buffer.NewValidatedBufferFromByteSlice([]byte("ell")))

		data, err := blobAccess.GetFromComposite(
			ctx,
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "3123059c1c816471780539f6b6b738dc", 3),
			blobSlicer,
		).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("ell"), data)
	})
}

func TestDemultiplexingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	demultiplexedBlobAccessGetter := mock.NewMockDemultiplexedBlobAccessGetter(ctrl)
	blobAccess := blobstore.NewDemultiplexingBlobAccess(demultiplexedBlobAccessGetter.Call)

	t.Run("UnknownInstanceName", func(t *testing.T) {
		// This request cannot be forwarded to any backend.
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("unknown")).Return(
			nil,
			"",
			digest.NoopInstanceNamePatcher,
			status.Error(codes.InvalidArgument, "Unknown instance name"))

		require.Equal(
			t,
			status.Error(codes.InvalidArgument, "Unknown instance name"),
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("unknown", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
				buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Error messages should have the backend name prepended.
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		baseBlobAccess.EXPECT().Put(ctx, digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5), gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "I/O error")
			})

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Backend \"Primary\": I/O error"),
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
				buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("Success", func(t *testing.T) {
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		baseBlobAccess.EXPECT().Put(ctx, digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5), gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			})

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
				buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestDemultiplexingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	demultiplexedBlobAccessGetter := mock.NewMockDemultiplexedBlobAccessGetter(ctrl)
	blobAccess := blobstore.NewDemultiplexingBlobAccess(demultiplexedBlobAccessGetter.Call)

	t.Run("UnknownInstanceName", func(t *testing.T) {
		// This request cannot be forwarded to any backend.
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("unknown")).Return(
			nil,
			"",
			digest.NoopInstanceNamePatcher,
			status.Error(codes.InvalidArgument, "Unknown instance name"))

		_, err := blobAccess.FindMissing(
			ctx,
			digest.MustNewDigest("unknown", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet())
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown instance name"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Error messages should have the backend name prepended.
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		baseBlobAccess.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("goodbye/world", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build()).
			Return(digest.EmptySet, status.Error(codes.Internal, "I/O error"))

		_, err := blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build())
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend \"Primary\": I/O error"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Call FindMissing() with 2x2x2 blobs, intended for
		// four different instance names spread across two
		// backends. We should see two FindMissing() calls
		// against the backends. Report half of the digests as
		// missing.
		baseBlobAccessA := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("a")).Return(
			baseBlobAccessA,
			"a",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("a"),
				digest.MustNewInstanceName("A")),
			nil)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("a/x")).Return(
			baseBlobAccessA,
			"a",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("a"),
				digest.MustNewInstanceName("A")),
			nil)
		baseBlobAccessB := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("b")).Return(
			baseBlobAccessB,
			"b",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("b"),
				digest.MustNewInstanceName("B")),
			nil)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("b/x")).Return(
			baseBlobAccessB,
			"b",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("b"),
				digest.MustNewInstanceName("B")),
			nil)
		baseBlobAccessA.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("A", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("A", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Add(digest.MustNewDigest("A/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("A/x", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build()).
			Return(
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("A", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
					Add(digest.MustNewDigest("A/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
					Build(),
				nil)
		baseBlobAccessB.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("B", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("B", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Add(digest.MustNewDigest("B/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("B/x", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build()).
			Return(
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("B", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
					Add(digest.MustNewDigest("B/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
					Build(),
				nil)

		missing, err := blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("a", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("a", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Add(digest.MustNewDigest("a/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("a/x", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Add(digest.MustNewDigest("b", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("b", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Add(digest.MustNewDigest("b/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("b/x", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build())
		require.NoError(t, err)
		require.Equal(
			t,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("a", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("a/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("b", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("b/x", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Build(),
			missing)
	})
}

func TestDemultiplexingBlobAccessGetCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	demultiplexedBlobAccessGetter := mock.NewMockDemultiplexedBlobAccessGetter(ctrl)
	blobAccess := blobstore.NewDemultiplexingBlobAccess(demultiplexedBlobAccessGetter.Call)

	t.Run("UnknownInstanceName", func(t *testing.T) {
		// This request cannot be forwarded to any backend.
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("unknown")).Return(
			nil,
			"",
			digest.NoopInstanceNamePatcher,
			status.Error(codes.InvalidArgument, "Unknown instance name"))

		_, err := blobAccess.GetCapabilities(ctx, digest.MustNewInstanceName("unknown"))
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown instance name"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Error messages should have the backend name prepended.
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		baseBlobAccess.EXPECT().GetCapabilities(ctx, digest.MustNewInstanceName("goodbye/world")).
			Return(nil, status.Error(codes.Internal, "Server offline"))

		_, err := blobAccess.GetCapabilities(ctx, digest.MustNewInstanceName("hello/world"))
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend \"Primary\": Server offline"), err)
	})

	t.Run("Success", func(t *testing.T) {
		baseBlobAccess := mock.NewMockBlobAccess(ctrl)
		demultiplexedBlobAccessGetter.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(
			baseBlobAccess,
			"Primary",
			digest.NewInstanceNamePatcher(
				digest.MustNewInstanceName("hello"),
				digest.MustNewInstanceName("goodbye")),
			nil)
		baseBlobAccess.EXPECT().GetCapabilities(ctx, digest.MustNewInstanceName("goodbye/world")).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
				},
			}, nil)

		serverCapabilities, err := blobAccess.GetCapabilities(ctx, digest.MustNewInstanceName("hello/world"))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
					UpdateEnabled: true,
				},
			},
		}, serverCapabilities)
	})
}
