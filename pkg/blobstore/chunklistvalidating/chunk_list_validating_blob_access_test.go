package chunklistvalidating_test

import (
	"bytes"
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklistvalidating"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mustComputeDigest is a test helper to easily generate digests from
// byte slices.
func mustComputeDigest(t *testing.T, digestFunction digest.Function, data []byte) digest.Digest {
	t.Helper()
	generator := digestFunction.NewGenerator(int64(len(data)))
	_, err := generator.Write(data)
	require.NoError(t, err)
	return generator.Sum()
}

var testCDCParams = &remoteexecution.RepMaxCdcParams{
	MinChunkSizeBytes: 1024,
	HorizonSizeBytes:  8 * 1024,
}
var maximumMessageSizeBytes = 1024 * 1024

func TestChunkListValidatingBlobAccessGetTrivialSmallBlob(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	blobData := []byte("Small trivial blob")
	blobDigest := mustComputeDigest(t, digestFunction, blobData)

	require.NoError(t, fakeCAS.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(blobData)))

	fakeCAS.ResetTouches()
	msg, err := validatingCLS.Get(ctx, blobDigest).ToProto(&remoteexecution.SplitBlobResponse{}, maximumMessageSizeBytes)
	require.NoError(t, err)

	splitResponse := msg.(*remoteexecution.SplitBlobResponse)
	require.Len(t, splitResponse.ChunkDigests, 1)
	require.Equal(t, blobDigest.GetProto().Hash, splitResponse.ChunkDigests[0].Hash)
	require.Greater(t, fakeCAS.GetTouches(blobDigest), 0, "Blob did not have its lifetime renewed.")
}

func TestChunkListValidatingBlobAccessGetExtendsLifetimes(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	blobData := bytes.Repeat([]byte("testdatafortests"), 250) // <4KiB
	chunk1Data := blobData[:len(blobData)/2]
	chunk2Data := blobData[len(blobData)/2:]

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	blobDigest := mustComputeDigest(t, digestFunction, blobData)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))
	require.NoError(t, fakeCAS.Put(ctx, chunk2Digest, buffer.NewValidatedBufferFromByteSlice(chunk2Data)))

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{
			chunk1Digest.GetProto(),
			chunk2Digest.GetProto(),
		},
	}
	chunkListBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)
	require.NoError(t, fakeCLS.Put(ctx, blobDigest, chunkListBuffer))

	// Reset touches.
	fakeCLS.ResetTouches()
	fakeCAS.ResetTouches()

	// Perform a cached split.
	msgCached, err := validatingCLS.Get(ctx, blobDigest).ToProto(&remoteexecution.SplitBlobResponse{}, maximumMessageSizeBytes)
	require.NoError(t, err)
	cachedResponse := msgCached.(*remoteexecution.SplitBlobResponse)
	require.Equal(t, len(splitResponse.ChunkDigests), len(cachedResponse.ChunkDigests))

	// The original blob MUST have had its lifetime extended
	require.Greater(t, fakeCLS.GetTouches(blobDigest), 0, "Original blob's chunk list lifetime was not extended during call to Get")

	// Every chunk MUST have had its lifetime extended
	for _, chunkProto := range cachedResponse.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkProto)
		require.NoError(t, err)
		require.Greater(t, fakeCAS.GetTouches(chunkDigest), 0, "Chunk's lifetime was not extended during call to Get")
	}
}

func TestChunkListValidatingBlobAccessGetLargeBlobMissingUnderlyingChunk(t *testing.T) {
	ctx := context.Background()
	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := bytes.Repeat([]byte("A"), 1500)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))
	chunk2Data := bytes.Repeat([]byte("B"), 1500)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	// Chunk 2 is not uploaded to the chunk storage

	expectedFullData := append(chunk1Data, chunk2Data...)
	blobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{
			chunk1Digest.GetProto(),
			chunk2Digest.GetProto(),
		},
	}
	manifestBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)
	require.NoError(t, fakeCLS.Put(ctx, blobDigest, manifestBuffer))

	_, err := validatingCLS.Get(ctx, blobDigest).ToProto(&remoteexecution.SplitBlobResponse{}, maximumMessageSizeBytes)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err), "Incorrect error message from Get request: %s", err.Error())
}

func TestChunkListValidatingBlobAccessPutManualSplice(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := []byte("Hello, ")
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))

	chunk2Data := []byte("World!")
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk2Digest, buffer.NewValidatedBufferFromByteSlice(chunk2Data)))

	expectedFullData := []byte("Hello, World!")
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{
			chunk1Digest.GetProto(),
			chunk2Digest.GetProto(),
		},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, fullBlobDigest, reqBuffer)
	require.NoError(t, err)

	composedData, err := fakeCAS.Get(ctx, fullBlobDigest).ToByteSlice(len(expectedFullData))
	require.NoError(t, err)
	require.Equal(t, expectedFullData, composedData)
}

func TestChunkListValidatingBlobAccessPutCanonicalization(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	blobData := bytes.Repeat([]byte("testdatafortests"), 250) // <4KiB
	chunk1Data := blobData[:len(blobData)/2]
	chunk2Data := blobData[len(blobData)/2:]

	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))

	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk2Digest, buffer.NewValidatedBufferFromByteSlice(chunk2Data)))

	fullBlobDigest := mustComputeDigest(t, digestFunction, blobData)

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{
			chunk1Digest.GetProto(),
			chunk2Digest.GetProto(),
		},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, fullBlobDigest, reqBuffer)
	require.NoError(t, err)

	canonicalBuffer := fakeCLS.Get(ctx, fullBlobDigest)
	canonicalProto, err := canonicalBuffer.ToProto(&remoteexecution.SplitBlobResponse{}, maximumMessageSizeBytes)
	require.NoError(t, err)

	canonicalResp := canonicalProto.(*remoteexecution.SplitBlobResponse)
	require.Greater(t, len(canonicalResp.ChunkDigests), 0)
	require.NotEqual(t, chunk1Digest.GetProto().Hash, canonicalResp.ChunkDigests[0].Hash, "Server should not have echoed back the non-standard chunks")
}

func TestChunkListValidatingBlobAccessPutMissingChunk(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	chunkDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{chunkDigest.GetProto()},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, chunkDigest, reqBuffer)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkListValidatingBlobAccessPutDigestMismatch(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkData := []byte("Valid chunk data")
	chunkDigest := mustComputeDigest(t, digestFunction, chunkData)
	require.NoError(t, fakeCAS.Put(ctx, chunkDigest, buffer.NewValidatedBufferFromByteSlice(chunkData)))

	wrongBlobDigest := mustComputeDigest(t, digestFunction, []byte("Different data"))

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{chunkDigest.GetProto()},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, wrongBlobDigest, reqBuffer)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "Incorrect error from Put request: %s", err.Error())
}

func TestChunkListValidatingBlobAccessPutEmptyBlob(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	emptyDigest := mustComputeDigest(t, digestFunction, nil)

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, emptyDigest, reqBuffer)
	require.NoError(t, err)
}

func TestChunkListValidatingBlobAccessPutRepeatedChunks(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkA := []byte("A")
	digestA := mustComputeDigest(t, digestFunction, chunkA)
	require.NoError(t, fakeCAS.Put(ctx, digestA, buffer.NewValidatedBufferFromByteSlice(chunkA)))

	chunkB := []byte("B")
	digestB := mustComputeDigest(t, digestFunction, chunkB)
	require.NoError(t, fakeCAS.Put(ctx, digestB, buffer.NewValidatedBufferFromByteSlice(chunkB)))

	expectedData := []byte("AABA")
	expectedDigest := mustComputeDigest(t, digestFunction, expectedData)

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{
			digestA.GetProto(),
			digestA.GetProto(),
			digestB.GetProto(),
			digestA.GetProto(),
		},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, expectedDigest, reqBuffer)
	require.NoError(t, err)

	composedData, err := fakeCAS.Get(ctx, expectedDigest).ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, expectedData, composedData)
}

func TestChunkListValidatingBlobAccessPutInlineEmptyChunk(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkData := []byte("Valid")
	chunkDigest := mustComputeDigest(t, digestFunction, chunkData)
	require.NoError(t, fakeCAS.Put(ctx, chunkDigest, buffer.NewValidatedBufferFromByteSlice(chunkData)))

	emptyDigest := mustComputeDigest(t, digestFunction, nil)
	require.NoError(t, fakeCAS.Put(ctx, emptyDigest, buffer.NewValidatedBufferFromByteSlice(nil)))

	expectedDigest := mustComputeDigest(t, digestFunction, chunkData)

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{
			chunkDigest.GetProto(),
			emptyDigest.GetProto(),
		},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, expectedDigest, reqBuffer)
	require.NoError(t, err)
}

func TestChunkListValidatingBlobAccessPutExtendsLifetimes(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := []byte("Hello, ")
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))

	chunk2Data := []byte("World!")
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk2Digest, buffer.NewValidatedBufferFromByteSlice(chunk2Data)))

	expectedFullData := []byte("Hello, World!")
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	fakeCAS.ResetTouches()

	splitResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests: []*remoteexecution.Digest{
			chunk1Digest.GetProto(),
			chunk2Digest.GetProto(),
		},
	}
	reqBuffer := buffer.NewProtoBufferFromProto(splitResponse, buffer.UserProvided)

	err := validatingCLS.Put(ctx, fullBlobDigest, reqBuffer)

	// From the REAPI, the server may either process the splice and
	// return OK, OR it may return ALREADY_EXISTS if the blob is already
	// composed and the server chooses not to extend the lifetime of the
	// user's specific chunks.
	if status.Code(err) == codes.AlreadyExists {
		// The server is free not to touch the user's chunks. However,
		// it MUST still have verified/touched the original blob.
		require.Greater(t, fakeCAS.GetTouches(fullBlobDigest), 0, "Composed blob lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCLS.GetTouches(fullBlobDigest), 0, "Composed blob chunk list lifetime was not extended during SpliceBlob")
	} else {
		// Because the server accepted the Splice request, it is
		// strictly obligated to extend the lifetimes of BOTH the
		// provided chunks and the composed blob.
		require.NoError(t, err)

		require.Greater(t, fakeCAS.GetTouches(chunk1Digest), 0, "Chunk 1 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCAS.GetTouches(chunk2Digest), 0, "Chunk 2 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCAS.GetTouches(fullBlobDigest), 0, "Composed blob lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCLS.GetTouches(fullBlobDigest), 0, "Composed blob chunk list lifetime was not extended during SpliceBlob")
	}
}

func TestChunkListValidatingBlobAccessGetMissingBlob(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(nil)
	fakeCLS := newFakeBlobAccess(testCDCParams)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	ghostDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	_, err := validatingCLS.Get(ctx, ghostDigest).ToProto(&remoteexecution.SplitBlobResponse{}, 1024*1024)

	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}
