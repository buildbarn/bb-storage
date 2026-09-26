package chunkmappingvalidating_test

import (
	"bytes"
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunkmappingvalidating"
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

// makeChunkMapping creates a chunk.Mapping from a list of chunk
// digests.
func makeChunkMapping(chunkDigests ...digest.Digest) chunk.Mapping {
	cl := chunk.Mapping{
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

func TestChunkMappingValidatingBlobAccessGetExtendsLifetimes(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	// The blob must be at least 2*MinChunkSizeBytes, so that Get
	// goes down the path of returning the stored chunk mapping.
	blobData := bytes.Repeat([]byte("a"), 2048)
	chunk1Data := blobData[:len(blobData)/2]
	chunk2Data := blobData[len(blobData)/2:]
	chunk1 := chunk.NewChunk(zstdPool, chunk1Data)
	chunk2 := chunk.NewChunk(zstdPool, chunk2Data)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	blobDigest := mustComputeDigest(t, digestFunction, blobData)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk2))
	require.NoError(t, fakeCMS.Put(ctx, blobDigest, makeChunkMapping(chunk1Digest, chunk2Digest)))

	// Reset touches.
	fakeCMS.ResetTouches()
	fakeCS.ResetTouches()

	// Perform a cached split.
	chunkMapping, err := validatingCMS.Get(ctx, blobDigest)
	require.NoError(t, err)
	require.Equal(t, []digest.Digest{chunk1Digest, chunk2Digest}, chunkMapping.Digests)

	// The original blob's chunk mapping MUST have had its lifetime
	// extended.
	require.Greater(t, fakeCMS.GetTouches(blobDigest), 0, "Original blob's chunk mapping lifetime was not extended during call to Get")

	// Every chunk MUST have had its lifetime extended.
	for _, chunkDigest := range chunkMapping.Digests {
		require.Greater(t, fakeCS.GetTouches(chunkDigest), 0, "Chunk's lifetime was not extended during call to Get")
	}
}

func TestChunkMappingValidatingBlobAccessGetLargeBlobMissingUnderlyingChunk(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Bytes := bytes.Repeat([]byte("A"), 1500)
	chunk1 := chunk.NewChunk(zstdPool, chunk1Bytes)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Bytes)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))
	chunk2Bytes := bytes.Repeat([]byte("B"), 1500)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Bytes)
	// Chunk 2 is not uploaded to the chunk storage.

	expectedFullData := append(chunk1Bytes, chunk2Bytes...)
	blobDigest := mustComputeDigest(t, digestFunction, expectedFullData)
	require.NoError(t, fakeCMS.Put(ctx, blobDigest, makeChunkMapping(chunk1Digest, chunk2Digest)))

	_, err := validatingCMS.Get(ctx, blobDigest)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err), "Incorrect error code from Get request: %s", err.Error())
}

func TestChunkMappingValidatingBlobAccessGetMissingBlob(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	ghostDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	_, err := validatingCMS.Get(ctx, ghostDigest)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkMappingValidatingBlobAccessPutManualSplice(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := []byte("Hello, ")
	chunk1 := chunk.NewChunk(zstdPool, chunk1Data)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))

	chunk2Data := []byte("World!")
	chunk2 := chunk.NewChunk(zstdPool, chunk2Data)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk2))

	expectedFullData := []byte("Hello, World!")
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	err := validatingCMS.Put(ctx, fullBlobDigest, makeChunkMapping(chunk1Digest, chunk2Digest))
	require.NoError(t, err)

	composedChunk, err := fakeCS.Get(ctx, fullBlobDigest)
	require.NoError(t, err)
	composedData := composedChunk.GetBytes()
	require.Equal(t, expectedFullData, composedData)
}

func TestChunkMappingValidatingBlobAccessPutCanonicalization(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	blobData := bytes.Repeat([]byte("testdatafortests"), 250)
	chunk1Data := blobData[:len(blobData)/2]
	chunk1 := chunk.NewChunk(zstdPool, chunk1Data)
	chunk2Data := blobData[len(blobData)/2:]
	chunk2 := chunk.NewChunk(zstdPool, chunk2Data)

	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk1))

	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk2))

	fullBlobDigest := mustComputeDigest(t, digestFunction, blobData)

	err := validatingCMS.Put(ctx, fullBlobDigest, makeChunkMapping(chunk1Digest, chunk2Digest))
	require.NoError(t, err)

	// The stored chunk mapping should be the canonical CDC chunking,
	// not the non-standard chunks that were provided.
	canonicalDigests, err := fakeCMS.Get(ctx, fullBlobDigest)
	require.NoError(t, err)
	require.Greater(t, len(canonicalDigests.Digests), 0)
	require.NotEqual(t, chunk1Digest, canonicalDigests.Digests[0], "Server should not have echoed back the non-standard chunks")
}

func TestChunkMappingValidatingBlobAccessPutMissingChunk(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	chunkDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	err := validatingCMS.Put(ctx, chunkDigest, makeChunkMapping(chunkDigest))
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkMappingValidatingBlobAccessPutDigestMismatch(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkData := []byte("Valid chunk data")
	chunkDigest := mustComputeDigest(t, digestFunction, chunkData)
	chunk := chunk.NewChunk(zstdPool, chunkData)
	require.NoError(t, fakeCS.Put(ctx, chunkDigest, chunk))

	wrongBlobDigest := mustComputeDigest(t, digestFunction, []byte("Different data"))

	err := validatingCMS.Put(ctx, wrongBlobDigest, makeChunkMapping(chunkDigest))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "Incorrect error code from Put request: %s", err.Error())
}

func TestChunkMappingValidatingBlobAccessPutEmptyBlob(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	emptyDigest := mustComputeDigest(t, digestFunction, nil)

	err := validatingCMS.Put(ctx, emptyDigest, makeChunkMapping())
	require.NoError(t, err)
}

func TestChunkMappingValidatingBlobAccessPutRepeatedChunks(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkAData := []byte("A")
	chunkA := chunk.NewChunk(zstdPool, chunkAData)
	digestA := mustComputeDigest(t, digestFunction, chunkAData)
	require.NoError(t, fakeCS.Put(ctx, digestA, chunkA))

	chunkBData := []byte("B")
	chunkB := chunk.NewChunk(zstdPool, chunkBData)
	digestB := mustComputeDigest(t, digestFunction, chunkBData)
	require.NoError(t, fakeCS.Put(ctx, digestB, chunkB))

	expectedData := []byte("AABA")
	expectedDigest := mustComputeDigest(t, digestFunction, expectedData)

	err := validatingCMS.Put(ctx, expectedDigest, makeChunkMapping(digestA, digestA, digestB, digestA))
	require.NoError(t, err)

	composedChunk, err := fakeCS.Get(ctx, expectedDigest)
	require.NoError(t, err)
	composedData := composedChunk.GetBytes()
	require.Equal(t, expectedData, composedData)
}

func TestChunkMappingValidatingBlobAccessPutExtendsLifetimeOfBlob(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	// Splicing small chunks must extend the lifetime of the chunk
	// that the blob canonically decomposes into, which is the blob
	// itself as it fits in a single chunk. The chunk mapping storage is
	// not involved, as no chunk mapping exists.
	chunk1Data := []byte("Hello, ")
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk.NewChunk(zstdPool, chunk1Data)))

	chunk2Data := []byte("World!")
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk.NewChunk(zstdPool, chunk2Data)))

	expectedFullData := []byte("Hello, World!")
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	fakeCS.ResetTouches()

	err := validatingCMS.Put(ctx, fullBlobDigest, makeChunkMapping(chunk1Digest, chunk2Digest))

	// From the REAPI, the server may either process the splice and
	// return OK, OR it may return ALREADY_EXISTS if the blob is
	// already composed and the server chooses not to extend the
	// lifetime of the user's specific chunks.
	if status.Code(err) == codes.AlreadyExists {
		// The server is free not to touch the user's chunks.
		// However, it MUST still have verified/touched the original
		// blob.
		require.Greater(t, fakeCS.GetTouches(fullBlobDigest), 0, "Composed blob lifetime was not extended during SpliceBlob")
	} else {
		// Because the server accepted the Splice request, it is
		// strictly obligated to extend the lifetimes of BOTH the
		// provided chunks and the composed blob.
		require.NoError(t, err)

		require.Greater(t, fakeCS.GetTouches(chunk1Digest), 0, "Chunk 1 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCS.GetTouches(chunk2Digest), 0, "Chunk 2 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCS.GetTouches(fullBlobDigest), 0, "Composed blob lifetime was not extended during SpliceBlob")
	}
}

func TestChunkMappingValidatingBlobAccessPutExtendsLifetimeOfChunkMapping(t *testing.T) {
	ctx := context.Background()

	fakeCS := newFakeBlobAccess[*chunk.Chunk](testCDCParams)
	fakeCMS := newFakeBlobAccess[chunk.Mapping](nil)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(fakeCMS, fakeCS, maximumMessageSizeBytes, zstdPool)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	// Splicing chunks that compose a blob which canonically
	// decomposes into at least two chunks must extend the lifetime
	// of the blob's chunk mapping. The blob itself is never stored as a
	// chunk, so the chunk storage is not involved.
	chunk1Data := bytes.Repeat([]byte("Hello, "), 220)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCS.Put(ctx, chunk1Digest, chunk.NewChunk(zstdPool, chunk1Data)))

	chunk2Data := bytes.Repeat([]byte("World! And so on. "), 90)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCS.Put(ctx, chunk2Digest, chunk.NewChunk(zstdPool, chunk2Data)))

	expectedFullData := append(chunk1Data, chunk2Data...)
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	fakeCS.ResetTouches()

	err := validatingCMS.Put(ctx, fullBlobDigest, makeChunkMapping(chunk1Digest, chunk2Digest))

	// From the REAPI, the server may either process the splice and
	// return OK, OR it may return ALREADY_EXISTS if the blob is
	// already composed and the server chooses not to extend the
	// lifetime of the user's specific chunks.
	if status.Code(err) == codes.AlreadyExists {
		// The server is free not to touch the user's chunks.
		// However, it MUST still have verified/touched the original
		// blob.
		require.Greater(t, fakeCMS.GetTouches(fullBlobDigest), 0, "Composed blob chunk mapping lifetime was not extended during SpliceBlob")
	} else {
		// Because the server accepted the Splice request, it is
		// strictly obligated to extend the lifetimes of BOTH the
		// provided chunks and the composed blob.
		require.NoError(t, err)

		require.Greater(t, fakeCS.GetTouches(chunk1Digest), 0, "Chunk 1 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCS.GetTouches(chunk2Digest), 0, "Chunk 2 lifetime was not extended during SpliceBlob")
		require.Greater(t, fakeCMS.GetTouches(fullBlobDigest), 0, "Composed blob chunk mapping lifetime was not extended during SpliceBlob")
	}
}
