package fallback_test

import (
	"context"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/fallback"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeBlobAccess struct {
	get              func(context.Context, digest.Digest) buffer.Buffer
	getFromComposite func(context.Context, digest.Digest, digest.Digest, slicing.BlobSlicer) buffer.Buffer
	put              func(context.Context, digest.Digest, buffer.Buffer) error
	findMissing      func(context.Context, digest.Set) (digest.Set, error)
	getCapabilities  func(context.Context, digest.InstanceName) (*remoteexecution.ServerCapabilities, error)
}

func (ba *fakeBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	return ba.get(ctx, d)
}

func (ba *fakeBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return ba.getFromComposite(ctx, parentDigest, childDigest, slicer)
}

func (ba *fakeBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	return ba.put(ctx, d, b)
}

func (ba *fakeBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.findMissing(ctx, digests)
}

func (ba *fakeBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return ba.getCapabilities(ctx, instanceName)
}

type fakeBlobSlicer struct{}

func (fakeBlobSlicer) Slice(b buffer.Buffer, childDigest digest.Digest) (buffer.Buffer, []slicing.BlobSlice) {
	return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "unused")), nil
}

func newRecordingPut(t *testing.T, expected []byte, returnErr error, after func()) func(context.Context, digest.Digest, buffer.Buffer) error {
	return func(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
		defer func() {
			if after != nil {
				after()
			}
		}()
		if returnErr != nil {
			b.Discard()
			return returnErr
		}
		data, err := b.ToByteSlice(len(expected) + 1)
		require.NoError(t, err)
		require.Equal(t, expected, data)
		return nil
	}
}

func TestFallbackBlobAccessGet(t *testing.T) {
	ctx := context.Background()

	helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("PrimarySuccess", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				get: func(context.Context, digest.Digest) buffer.Buffer {
					return buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))
				},
			},
			&fakeBlobAccess{},
		)

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("PrimaryUnavailableSecondarySuccess", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				get: func(context.Context, digest.Digest) buffer.Buffer {
					return buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server offline"))
				},
			},
			&fakeBlobAccess{
				get: func(context.Context, digest.Digest) buffer.Buffer {
					return buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))
				},
			},
		)

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("PrimaryFailure", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				get: func(context.Context, digest.Digest) buffer.Buffer {
					return buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error"))
				},
			},
			&fakeBlobAccess{},
		)

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Primary: I/O error"), err)
	})

	t.Run("SecondaryFailure", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				get: func(context.Context, digest.Digest) buffer.Buffer {
					return buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server offline"))
				},
			},
			&fakeBlobAccess{
				get: func(context.Context, digest.Digest) buffer.Buffer {
					return buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error"))
				},
			},
		)

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Secondary: I/O error"), err)
	})
}

func TestFallbackBlobAccessGetFromComposite(t *testing.T) {
	ctx := context.Background()

	parentDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d20fb8dfa347cf895b38649410aeb3f8", 100)
	childDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	slicer := fakeBlobSlicer{}
	blobAccess := fallback.NewFallbackBlobAccess(
		&fakeBlobAccess{
			getFromComposite: func(context.Context, digest.Digest, digest.Digest, slicing.BlobSlicer) buffer.Buffer {
				return buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server offline"))
			},
		},
		&fakeBlobAccess{
			getFromComposite: func(context.Context, digest.Digest, digest.Digest, slicing.BlobSlicer) buffer.Buffer {
				return buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))
			},
		},
	)

	data, err := blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)
}

func TestFallbackBlobAccessPut(t *testing.T) {
	ctx := context.Background()

	helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("PrimarySuccessBestEffortSecondary", func(t *testing.T) {
		var secondaryDone sync.WaitGroup
		secondaryDone.Add(1)
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				put: newRecordingPut(t, []byte("Hello"), nil, nil),
			},
			&fakeBlobAccess{
				put: newRecordingPut(t, []byte("Hello"), status.Error(codes.Internal, "Ignored"), secondaryDone.Done),
			},
		)
		require.NoError(t, blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
		secondaryDone.Wait()
	})

	t.Run("PrimaryUnavailableSecondarySuccess", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				put: newRecordingPut(t, nil, status.Error(codes.Unavailable, "Server offline"), nil),
			},
			&fakeBlobAccess{
				put: newRecordingPut(t, []byte("Hello"), nil, nil),
			},
		)
		require.NoError(t, blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("PrimaryFailure", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				put: newRecordingPut(t, nil, status.Error(codes.Internal, "I/O error"), nil),
			},
			&fakeBlobAccess{},
		)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Primary: I/O error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("SecondaryFailureAfterFallback", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				put: newRecordingPut(t, nil, status.Error(codes.Unavailable, "Server offline"), nil),
			},
			&fakeBlobAccess{
				put: newRecordingPut(t, nil, status.Error(codes.Internal, "I/O error"), nil),
			},
		)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Secondary: I/O error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestFallbackBlobAccessFindMissing(t *testing.T) {
	ctx := context.Background()

	allDigests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "00000000000000000000000000000000", 100)).
		Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "00000000000000000000000000000001", 101)).
		Build()
	missing := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "00000000000000000000000000000000", 100).ToSingletonSet()

	t.Run("PrimarySuccess", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				findMissing: func(context.Context, digest.Set) (digest.Set, error) {
					return missing, nil
				},
			},
			&fakeBlobAccess{},
		)

		result, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, missing, result)
	})

	t.Run("PrimaryUnavailableSecondarySuccess", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				findMissing: func(context.Context, digest.Set) (digest.Set, error) {
					return digest.EmptySet, status.Error(codes.Unavailable, "Server offline")
				},
			},
			&fakeBlobAccess{
				findMissing: func(context.Context, digest.Set) (digest.Set, error) {
					return missing, nil
				},
			},
		)

		result, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, missing, result)
	})

	t.Run("PrimaryFailure", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				findMissing: func(context.Context, digest.Set) (digest.Set, error) {
					return digest.EmptySet, status.Error(codes.Internal, "I/O error")
				},
			},
			&fakeBlobAccess{},
		)

		_, err := blobAccess.FindMissing(ctx, allDigests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Primary: I/O error"), err)
	})

	t.Run("SecondaryFailure", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				findMissing: func(context.Context, digest.Set) (digest.Set, error) {
					return digest.EmptySet, status.Error(codes.Unavailable, "Server offline")
				},
			},
			&fakeBlobAccess{
				findMissing: func(context.Context, digest.Set) (digest.Set, error) {
					return digest.EmptySet, status.Error(codes.Internal, "I/O error")
				},
			},
		)

		_, err := blobAccess.FindMissing(ctx, allDigests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Secondary: I/O error"), err)
	})
}

func TestFallbackBlobAccessGetCapabilities(t *testing.T) {
	ctx := context.Background()

	instanceName, err := digest.NewInstanceName("instance")
	require.NoError(t, err)
	capabilities := &remoteexecution.ServerCapabilities{}

	t.Run("PrimarySuccess", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				getCapabilities: func(context.Context, digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
					return capabilities, nil
				},
			},
			&fakeBlobAccess{},
		)

		result, err := blobAccess.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		require.Equal(t, capabilities, result)
	})

	t.Run("PrimaryUnavailableSecondarySuccess", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				getCapabilities: func(context.Context, digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
					return nil, status.Error(codes.Unavailable, "Server offline")
				},
			},
			&fakeBlobAccess{
				getCapabilities: func(context.Context, digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
					return capabilities, nil
				},
			},
		)

		result, err := blobAccess.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		require.Equal(t, capabilities, result)
	})

	t.Run("PrimaryFailure", func(t *testing.T) {
		blobAccess := fallback.NewFallbackBlobAccess(
			&fakeBlobAccess{
				getCapabilities: func(context.Context, digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
					return nil, status.Error(codes.Internal, "I/O error")
				},
			},
			&fakeBlobAccess{},
		)

		_, err := blobAccess.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Primary: I/O error"), err)
	})
}
