package chunklistvalidating_test

import (
	"bytes"
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklistvalidating"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
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

// makeChunkList creates a chunklist.ChunkList from a list of chunk
// digests.
func makeChunkList(chunkDigests ...digest.Digest) chunklist.ChunkList {
	cl := chunklist.ChunkList{
		Digests: chunkDigests,
		Offsets: make([]uint64, len(chunkDigests)),
	}
	var offset uint64
	for i, d := range chunkDigests {
		cl.Offsets[i] = offset
		offset += uint64(d.GetSizeBytes())
	}
	return cl
}

var testCDCParams = &remoteexecution.RepMaxCdcParams{
	MinChunkSizeBytes: 1024,
	HorizonSizeBytes:  8 * 1024,
}
var maximumMessageSizeBytes = 1024 * 1024

func TestChunkListValidatingBlobAccessGetTrivialSmallBlob(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	chunk1Data := []byte("Small trivial blob")
	chunk := buffer.NewChunk(zstdPool, chunk1Data)
	blobDigest := mustComputeDigest(t, digestFunction, chunk1Data)

	require.NoError(t, fakeCS.Put(ctx, blobDigest, chunk))

	fakeCS.ResetTouches()
	chunkList, err := validatingCLS.Get(ctx, blobDigest)
	require.NoError(t, err)

	require.Len(t, chunkList.Digests, 1)
	require.Equal(t, blobDigest, chunkList.Digests[0])
	require.Greater(t, fakeCS.GetTouches(blobDigest), 0, "Blob did not have its lifetime renewed.")
}

func TestChunkListValidatingBlobAccessGetExtendsLifetimes(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	// The blob must be at least 2*MinChunkSizeBytes, so that Get
	// goes down the path of returning the stored chunk list.
	blobData := bytes.Repeat([]byte("a"), 2048)
	chunk1Data := blobData[:len(blobData)/2]
	chunk2Data := blobData[len(blobData)/2:]
	chunk1 := buffer.NewChunk(zstdPool, chunk1Data)
	chunk2 := buffer.NewChunk(zstdPool, chunk2Data)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	blobDigest := mustComputeDigest(t, digestFunction, blobData)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk2))
	require.NoError(t, fakeCLS.Put(ctx, blobDigest, makeChunkList(chunk1Digest, chunk2Digest)))

	// Reset touches.
	fakeCLS.ResetTouches()
	fakeCS.ResetTouches()

	// Perform a cached split.
	chunkList, err := validatingCLS.Get(ctx, blobDigest)
	require.NoError(t, err)
	require.Equal(t, []digest.Digest{chunk1Digest, chunk2Digest}, chunkList.Digests)

	// The original blob's chunk list MUST have had its lifetime
	// extended.
	require.Greater(t, fakeCLS.GetTouches(blobDigest), 0, "Original blob's chunk list lifetime was not extended during call to Get")

	// Every chunk MUST have had its lifetime extended.
	for _, chunkDigest := range chunkList.Digests {
		require.Greater(t, fakeCS.GetTouches(chunkDigest), 0, "Chunk's lifetime was not extended during call to Get")
	}
}

func TestChunkListValidatingBlobAccessGetLargeBlobMissingUnderlyingChunk(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Bytes := bytes.Repeat([]byte("A"), 1500)
	chunk1 := buffer.NewChunk(zstdPool, chunk1Bytes)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Bytes)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))
	chunk2Bytes := bytes.Repeat([]byte("B"), 1500)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Bytes)
	// Chunk 2 is not uploaded to the chunk storage.

	expectedFullData := append(chunk1Bytes, chunk2Bytes...)
	blobDigest := mustComputeDigest(t, digestFunction, expectedFullData)
	require.NoError(t, fakeCLS.Put(ctx, blobDigest, makeChunkList(chunk1Digest, chunk2Digest)))

	_, err := validatingCLS.Get(ctx, blobDigest)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err), "Incorrect error code from Get request: %s", err.Error())
}

func TestChunkListValidatingBlobAccessGetMissingBlob(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	ghostDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	_, err := validatingCLS.Get(ctx, ghostDigest)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkListValidatingBlobAccessPutManualSplice(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := []byte("Hello, ")
	chunk1 := buffer.NewChunk(zstdPool, chunk1Data)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))

	chunk2Data := []byte("World!")
	chunk2 := buffer.NewChunk(zstdPool, chunk2Data)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk2))

	expectedFullData := []byte("Hello, World!")
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	err := validatingCLS.Put(ctx, fullBlobDigest, makeChunkList(chunk1Digest, chunk2Digest))
	require.NoError(t, err)

	composedChunk, err := fakeCS.Get(ctx, fullBlobDigest)
	require.NoError(t, err)
	composedData, err := composedChunk.GetBytes(ctx)
	require.Equal(t, expectedFullData, composedData)
}

func TestChunkListValidatingBlobAccessPutCanonicalization(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	blobData := bytes.Repeat([]byte("testdatafortests"), 250)
	chunk1Data := blobData[:len(blobData)/2]
	chunk1 := buffer.NewChunk(zstdPool, chunk1Data)
	chunk2Data := blobData[len(blobData)/2:]
	chunk2 := buffer.NewChunk(zstdPool, chunk2Data)

	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))

	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk2))

	fullBlobDigest := mustComputeDigest(t, digestFunction, blobData)

	err := validatingCLS.Put(ctx, fullBlobDigest, makeChunkList(chunk1Digest, chunk2Digest))
	require.NoError(t, err)

	// The stored chunk list should be the canonical CDC chunking,
	// not the non-standard chunks that were provided.
	canonicalDigests, err := fakeCLS.Get(ctx, fullBlobDigest)
	require.NoError(t, err)
	require.Greater(t, len(canonicalDigests.Digests), 0)
	require.NotEqual(t, chunk1Digest, canonicalDigests.Digests[0], "Server should not have echoed back the non-standard chunks")
}

func TestChunkListValidatingBlobAccessPutMissingChunk(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	chunkDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	err := validatingCLS.Put(ctx, chunkDigest, makeChunkList(chunkDigest))
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkListValidatingBlobAccessPutDigestMismatch(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkData := []byte("Valid chunk data")
	chunkDigest := mustComputeDigest(t, digestFunction, chunkData)
	chunk := buffer.NewChunk(zstdPool, chunkData)
	require.NoError(t, fakeCS.Put(ctx, chunkDigest, chunk))

	wrongBlobDigest := mustComputeDigest(t, digestFunction, []byte("Different data"))

	err := validatingCLS.Put(ctx, wrongBlobDigest, makeChunkList(chunkDigest))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "Incorrect error code from Put request: %s", err.Error())
}

func TestChunkListValidatingBlobAccessPutEmptyBlob(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	emptyDigest := mustComputeDigest(t, digestFunction, nil)

	err := validatingCLS.Put(ctx, emptyDigest, makeChunkList())
	require.NoError(t, err)
}

func TestChunkListValidatingBlobAccessPutRepeatedChunks(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkAData := []byte("A")
	chunkA := buffer.NewChunk(zstdPool, chunkAData)
	digestA := mustComputeDigest(t, digestFunction, chunkAData)
	require.NoError(t, fakeCS.Put(ctx, digestA, chunkA))

	chunkBData := []byte("B")
	chunkB := buffer.NewChunk(zstdPool, chunkBData)
	digestB := mustComputeDigest(t, digestFunction, chunkBData)
	require.NoError(t, fakeCS.Put(ctx, digestB, chunkB))

	expectedData := []byte("AABA")
	expectedDigest := mustComputeDigest(t, digestFunction, expectedData)

	err := validatingCLS.Put(ctx, expectedDigest, makeChunkList(digestA, digestA, digestB, digestA))
	require.NoError(t, err)

	composedChunk, err := fakeCS.Get(ctx, expectedDigest)
	require.NoError(t, err)
	composedData, err := composedChunk.GetBytes(ctx)
	require.Equal(t, expectedData, composedData)
}

func TestChunkListValidatingBlobAccessPutInlineEmptyChunk(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkData := []byte("Valid")
	chunk := buffer.NewChunk(zstdPool, chunkData)
	chunkDigest := mustComputeDigest(t, digestFunction, chunkData)
	require.NoError(t, fakeCS.Put(ctx, chunkDigest, chunk))

	emptyDigest := mustComputeDigest(t, digestFunction, nil)
	require.NoError(t, fakeCS.Put(ctx, emptyDigest, buffer.EmptyChunk))

	expectedDigest := mustComputeDigest(t, digestFunction, chunkData)

	err := validatingCLS.Put(ctx, expectedDigest, makeChunkList(chunkDigest, emptyDigest))
	require.NoError(t, err)
}

func TestChunkListValidatingBlobAccessPutExtendsLifetimes(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*buffer.Chunk](testCDCParams)
	fakeCLS := newFakeBlobAccess[chunklist.ChunkList](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := []byte("Hello, ")
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, buffer.NewChunk(zstdPool, chunk1Data)))

	chunk2Data := []byte("World!")
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, buffer.NewChunk(zstdPool, chunk2Data)))

	expectedFullData := []byte("Hello, World!")
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	fakeCS.ResetTouches()

	err := validatingCLS.Put(ctx, fullBlobDigest, makeChunkList(chunk1Digest, chunk2Digest))

	// From the REAPI, the server may either process the splice and
	// return OK, OR it may return ALREADY_EXISTS if the blob is
	// already composed and the server chooses not to extend the
	// lifetime of the user's specific chunks.
	if status.Code(err) == codes.AlreadyExists {
		// The server is free not to touch the user's chunks.
		// However, it MUST still have verified/touched the original
		// blob.
		require.Greater(t, fakeCS.GetTouches(fullBlobDigest), 0, "Composed blob lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCLS.GetTouches(fullBlobDigest), 0, "Composed blob chunk list lifetime was not extended during SpliceBlob")
	} else {
		// Because the server accepted the Splice request, it is
		// strictly obligated to extend the lifetimes of BOTH the
		// provided chunks and the composed blob.
		require.NoError(t, err)

		require.Greater(t, fakeCS.GetTouches(chunk1Digest), 0, "Chunk 1 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCS.GetTouches(chunk2Digest), 0, "Chunk 2 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCS.GetTouches(fullBlobDigest), 0, "Composed blob lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCLS.GetTouches(fullBlobDigest), 0, "Composed blob chunk list lifetime was not extended during SpliceBlob")
	}
}
