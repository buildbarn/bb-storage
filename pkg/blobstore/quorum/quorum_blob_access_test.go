package quorum_test

import (
	"context"
	"sync/atomic"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/quorum"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func setup(t *testing.T) (*gomock.Controller, context.Context, []*mock.MockBlobAccess, blobstore.BlobAccess) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

    mockBackends := make([]*mock.MockBlobAccess, 3)
	backends := make([]blobstore.BlobAccess, 3)
	for i := range backends {
        b := mock.NewMockBlobAccess(ctrl)
		mockBackends[i] = b
		backends[i] = b
	}

	blobAccess := quorum.NewQuorumBlobAccess(backends, 2, 2)

	return ctrl, ctx, mockBackends, blobAccess
}

var blobDigest = digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

func TestQuorumBlobAccessGet_Success(t *testing.T) {
	// Common case: Blob exists on a quorum of backends.
	ctrl, ctx, backends, blobAccess := setup(t)

	backends[0].EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))).MinTimes(1)
	backends[1].EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))).MinTimes(1)
	backends[2].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).MinTimes(1)

	// Requests should alternate between backends to spread
	// the load between backends equally.
	for i := 0; i < 100 && !ctrl.Satisfied(); i++ {
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	}
}

func TestQuorumBlobAccessGet_NotFoundAll(t *testing.T) {
	// Common case: Blob is not present on any backend.
	_, ctx, backends, blobAccess := setup(t)

	backends[0].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).MaxTimes(1)
	backends[1].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).MaxTimes(1)
	backends[2].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).MaxTimes(1)

	_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
}

func TestQuorumBlobAccessGet_UnreachableSuccess(t *testing.T) {
	// One backend unavailable. Should query remaining two, and find blob.
	_, ctx, backends, blobAccess := setup(t)

	backends[0].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch"))).MaxTimes(1)
	backends[1].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).MaxTimes(1)
	backends[2].EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))).Times(1)

	data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)
}

func TestQuorumBlobAccessGet_UnreachableNotFound(t *testing.T) {
	// One backend unavailable. Should query remaining two, and conclude not found.
	_, ctx, backends, blobAccess := setup(t)

	backends[0].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch"))).MaxTimes(1)
	backends[1].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).Times(1)
	backends[2].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).Times(1)

	_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
}

func TestQuorumBlobAccessGet_Unreachable2Success(t *testing.T) {
	// Two backends unavailable. Should query remaining one, and find blob.
	_, ctx, backends, blobAccess := setup(t)

	backends[0].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch"))).MaxTimes(1)
	backends[1].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch"))).MaxTimes(1)
	backends[2].EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))).Times(1)

	data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)
}

func TestQuorumBlobAccessGet_Unreachable2Failure(t *testing.T) {
	// Two backends unavailable. Should query remaining one, but be unable to conclude not found.
	_, ctx, backends, blobAccess := setup(t)

	backends[0].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch")))
	backends[1].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch")))
	backends[2].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).Times(1)

	// Can't conclude the blob doesn't exist.
	_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Too many backends unavailable"), err)
}

func TestQuorumBlobAccessGet_Unreachable3Failure(t *testing.T) {
	// All backends unavailable.
	_, ctx, backends, blobAccess := setup(t)

	backends[0].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch"))).Times(1)
	backends[1].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch"))).Times(1)
	backends[2].EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server gone to lunch"))).Times(1)

	// Can't conclude the blob doesn't exist.
	_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Too many backends unavailable"), err)
}

func TestQuorumBlobAccessGetFromComposite_Success(t *testing.T) {
	// Common case: Blob exists on a quorum of backends.
	//
	// We assume that tests for Get() provides coverage for other
	// scenarios.
	ctrl, ctx, backends, blobAccess := setup(t)

	parentDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "834c514174f3a7d5952dfa68d4b657f3c4cf78b3973dcf2721731c3861559828", 100)
	childDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	slicer := mock.NewMockBlobSlicer(ctrl)

	backends[0].EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))).MaxTimes(1)
	backends[1].EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))).MaxTimes(1)
	backends[2].EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))).MaxTimes(1)

	data, err := blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)
}

func TestQuorumBlobAccessPut_Success(t *testing.T) {
	_, ctx, backends, blobAccess := setup(t)

	var numWrites atomic.Int32
	doSuccess := func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
		data, err := b.ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
		numWrites.Add(1)
		return nil
	}

	backends[0].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doSuccess).MaxTimes(1)
	backends[1].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doSuccess).MaxTimes(1)
	backends[2].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doSuccess).MaxTimes(1)

	require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	require.EqualValues(t, 2, numWrites.Load())
}

func TestQuorumBlobAccessPut_UnreachableSuccess(t *testing.T) {
	// One backend unavailable.  Should write to remaining two.
	_, ctx, backends, blobAccess := setup(t)

	var numWrites atomic.Int32
	doSuccess := func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
		b.Discard()
		numWrites.Add(1)
		return nil
	}
	doUnavail := func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
		b.Discard()
		return status.Error(codes.Unavailable, "Server gone to lunch")
	}

	backends[0].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doSuccess).Times(1)
	backends[1].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doSuccess).Times(1)
	backends[2].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doUnavail).MaxTimes(1)

	require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	require.EqualValues(t, 2, numWrites.Load())
}

func TestQuorumBlobAccessPut_Unreachable2Failure(t *testing.T) {
	// Two backends unavailable.  Should not report success.
	_, ctx, backends, blobAccess := setup(t)

	doSuccess := func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
		b.Discard()
		return nil
	}
	doUnavail := func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
		b.Discard()
		return status.Error(codes.Unavailable, "Server gone to lunch")
	}

	backends[0].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doSuccess).MaxTimes(1)
	backends[1].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doUnavail).MaxTimes(1)
	backends[2].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doUnavail).MaxTimes(1)

	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unavailable, "Server gone to lunch"),
		blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
}

func TestQuorumBlobAccessPut_Failure(t *testing.T) {
	// Non-infrastructure error.  Should return error cause.
	_, ctx, backends, blobAccess := setup(t)

	doFail := func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
		b.Discard()
		return status.Error(codes.InvalidArgument, "Computer says no")
	}

	backends[0].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doFail).MaxTimes(1)
	backends[1].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doFail).MaxTimes(1)
	backends[2].EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(doFail).MaxTimes(1)

	testutil.RequireEqualStatus(
		t,
		status.Error(codes.InvalidArgument, "Computer says no"),
		blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
}

func TestQuorumBlobAccessFindMissing_Success(t *testing.T) {
	_, ctx, backends, blobAccess := setup(t)

	digestNone := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	digestA := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	digestB := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2", 9)
	allDigests := digest.NewSetBuilder().Add(digestNone).Add(digestA).Add(digestB).Build()
	missingFrom0 := digest.NewSetBuilder().Add(digestNone).Add(digestA).Build() // Missing A
	missingFrom1 := digest.NewSetBuilder().Add(digestNone).Add(digestB).Build() // Missing B
	missingFrom2 := digest.NewSetBuilder().Add(digestNone).Build()              // Has A and B

	backends[0].EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFrom0, nil).MaxTimes(1)
	backends[1].EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFrom1, nil).MaxTimes(1)
	backends[2].EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFrom2, nil).MaxTimes(1)

	missing, err := blobAccess.FindMissing(ctx, allDigests)
	require.NoError(t, err)
	require.Equal(t, digestNone.ToSingletonSet(), missing)
}

func TestQuorumBlobAccessFindMissing_UnavailableSuccess(t *testing.T) {
    // One server unavailable.  Doesn't change result.
	_, ctx, backends, blobAccess := setup(t)

	digestNone := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	digestA := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	digestB := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2", 9)
	allDigests := digest.NewSetBuilder().Add(digestNone).Add(digestA).Add(digestB).Build()
    missingFrom0 := digest.NewSetBuilder().Add(digestNone).Add(digestA).Build() // Missing A
	missingFrom1 := digest.NewSetBuilder().Add(digestNone).Add(digestB).Build() // Missing B
	//missingFrom2 := digest.NewSetBuilder().Add(digestNone).Build()            // Has A and B

	backends[0].EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFrom0, nil).MaxTimes(1)
	backends[1].EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFrom1, nil).MaxTimes(1)
	backends[2].EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Unavailable, "Server gone to lunch")).MaxTimes(1)

	missing, err := blobAccess.FindMissing(ctx, allDigests)
	require.NoError(t, err)
	require.Equal(t, digestNone.ToSingletonSet(), missing)
}

func TestQuorumBlobAccessFindMissing_Unavailable2Failure(t *testing.T) {
    // Two servers unavailable.  Return failure.
	_, ctx, backends, blobAccess := setup(t)

	digestNone := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	digestA := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	digestB := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2", 9)
	allDigests := digest.NewSetBuilder().Add(digestNone).Add(digestA).Add(digestB).Build()
    missingFrom0 := digest.NewSetBuilder().Add(digestNone).Add(digestA).Build() // Missing A

	backends[0].EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFrom0, nil).MaxTimes(1)
	backends[1].EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Unavailable, "Server gone to lunch")).MaxTimes(1)
	backends[2].EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Unavailable, "Server gone to lunch")).MaxTimes(1)

	_, err := blobAccess.FindMissing(ctx, allDigests)
    testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server gone to lunch"), err)
}
