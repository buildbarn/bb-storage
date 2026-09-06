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
	chunklist_pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/chunklist"
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

// makeChunkListBuffer creates a buffer containing a ChunkList proto
// from a list of chunk digests, suitable for passing to Put().
func makeChunkListBuffer(chunkDigests ...digest.Digest) buffer.Buffer {
	cl := make(chunklist.ChunkList, 0, len(chunkDigests))
	var offset uint64
	for _, d := range chunkDigests {
		cl = append(cl, chunklist.Entry{Offset: offset, Digest: d})
		offset += uint64(d.GetSizeBytes())
	}
	return buffer.NewProtoBufferFromProto(chunklist.ToProto(cl), buffer.UserProvided)
}

// getChunkListDigests reads a ChunkList proto from a buffer and
// returns the chunk digests it contains.
func getChunkListDigests(t *testing.T, buf buffer.Buffer, instanceName digest.InstanceName) []digest.Digest {
	t.Helper()
	msg, err := buf.ToProto(&chunklist_pb.ChunkList{}, maximumMessageSizeBytes)
	require.NoError(t, err)
	cl, err := chunklist.NewChunkListFromProto(msg.(*chunklist_pb.ChunkList), instanceName)
	require.NoError(t, err)
	digests := make([]digest.Digest, 0, len(cl))
	for _, entry := range cl {
		digests = append(digests, entry.Digest)
	}
	return digests
}

var testCDCParams = &remoteexecution.RepMaxCdcParams{
	MinChunkSizeBytes: 1024,
	HorizonSizeBytes:  8 * 1024,
}
var maximumMessageSizeBytes = 1024 * 1024

func TestChunkListValidatingBlobAccessGetTrivialSmallBlob(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	blobData := []byte("Small trivial blob")
	blobDigest := mustComputeDigest(t, digestFunction, blobData)

	require.NoError(t, fakeCAS.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(blobData)))

	fakeCAS.ResetTouches()
	chunkDigests := getChunkListDigests(t, validatingCLS.Get(ctx, blobDigest), digestFunction.GetInstanceName())

	require.Len(t, chunkDigests, 1)
	require.Equal(t, blobDigest, chunkDigests[0])
	require.Greater(t, fakeCAS.GetTouches(blobDigest), 0, "Blob did not have its lifetime renewed.")
}

func TestChunkListValidatingBlobAccessGetExtendsLifetimes(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	// The blob must be at least 2*MinChunkSizeBytes, so that Get
	// goes down the path of returning the stored chunk list.
	blobData := bytes.Repeat([]byte("testdatafortests"), 250)
	blobData = blobData[:2048]
	chunk1Data := blobData[:len(blobData)/2]
	chunk2Data := blobData[len(blobData)/2:]

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	blobDigest := mustComputeDigest(t, digestFunction, blobData)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))
	require.NoError(t, fakeCAS.Put(ctx, chunk2Digest, buffer.NewValidatedBufferFromByteSlice(chunk2Data)))
	require.NoError(t, fakeCLS.Put(ctx, blobDigest, makeChunkListBuffer(chunk1Digest, chunk2Digest)))

	// Reset touches.
	fakeCLS.ResetTouches()
	fakeCAS.ResetTouches()

	// Perform a cached split.
	cachedDigests := getChunkListDigests(t, validatingCLS.Get(ctx, blobDigest), digestFunction.GetInstanceName())
	require.Equal(t, []digest.Digest{chunk1Digest, chunk2Digest}, cachedDigests)

	// The original blob's chunk list MUST have had its lifetime
	// extended.
	require.Greater(t, fakeCLS.GetTouches(blobDigest), 0, "Original blob's chunk list lifetime was not extended during call to Get")

	// Every chunk MUST have had its lifetime extended.
	for _, chunkDigest := range cachedDigests {
		require.Greater(t, fakeCAS.GetTouches(chunkDigest), 0, "Chunk's lifetime was not extended during call to Get")
	}
}

func TestChunkListValidatingBlobAccessGetLargeBlobMissingUnderlyingChunk(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := bytes.Repeat([]byte("A"), 1500)
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))
	chunk2Data := bytes.Repeat([]byte("B"), 1500)
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	// Chunk 2 is not uploaded to the chunk storage.

	expectedFullData := append(chunk1Data, chunk2Data...)
	blobDigest := mustComputeDigest(t, digestFunction, expectedFullData)
	require.NoError(t, fakeCLS.Put(ctx, blobDigest, makeChunkListBuffer(chunk1Digest, chunk2Digest)))

	_, err := validatingCLS.Get(ctx, blobDigest).ToProto(&chunklist_pb.ChunkList{}, maximumMessageSizeBytes)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err), "Incorrect error code from Get request: %s", err.Error())
}

func TestChunkListValidatingBlobAccessGetMissingBlob(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	ghostDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	_, err := validatingCLS.Get(ctx, ghostDigest).ToProto(&chunklist_pb.ChunkList{}, maximumMessageSizeBytes)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkListValidatingBlobAccessPutManualSplice(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
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

	err := validatingCLS.Put(ctx, fullBlobDigest, makeChunkListBuffer(chunk1Digest, chunk2Digest))
	require.NoError(t, err)

	composedData, err := fakeCAS.Get(ctx, fullBlobDigest).ToByteSlice(len(expectedFullData))
	require.NoError(t, err)
	require.Equal(t, expectedFullData, composedData)
}

func TestChunkListValidatingBlobAccessPutCanonicalization(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	blobData := bytes.Repeat([]byte("testdatafortests"), 250)
	chunk1Data := blobData[:len(blobData)/2]
	chunk2Data := blobData[len(blobData)/2:]

	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk1Digest, buffer.NewValidatedBufferFromByteSlice(chunk1Data)))

	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)
	require.NoError(t, fakeCAS.Put(ctx, chunk2Digest, buffer.NewValidatedBufferFromByteSlice(chunk2Data)))

	fullBlobDigest := mustComputeDigest(t, digestFunction, blobData)

	err := validatingCLS.Put(ctx, fullBlobDigest, makeChunkListBuffer(chunk1Digest, chunk2Digest))
	require.NoError(t, err)

	// The stored chunk list should be the canonical CDC chunking,
	// not the non-standard chunks that were provided.
	canonicalDigests := getChunkListDigests(t, fakeCLS.Get(ctx, fullBlobDigest), digestFunction.GetInstanceName())
	require.Greater(t, len(canonicalDigests), 0)
	require.NotEqual(t, chunk1Digest, canonicalDigests[0], "Server should not have echoed back the non-standard chunks")
}

func TestChunkListValidatingBlobAccessPutMissingChunk(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	chunkDigest := mustComputeDigest(t, digestFunction, []byte("ghost"))

	err := validatingCLS.Put(ctx, chunkDigest, makeChunkListBuffer(chunkDigest))
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkListValidatingBlobAccessPutDigestMismatch(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkData := []byte("Valid chunk data")
	chunkDigest := mustComputeDigest(t, digestFunction, chunkData)
	require.NoError(t, fakeCAS.Put(ctx, chunkDigest, buffer.NewValidatedBufferFromByteSlice(chunkData)))

	wrongBlobDigest := mustComputeDigest(t, digestFunction, []byte("Different data"))

	err := validatingCLS.Put(ctx, wrongBlobDigest, makeChunkListBuffer(chunkDigest))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "Incorrect error code from Put request: %s", err.Error())
}

func TestChunkListValidatingBlobAccessPutEmptyBlob(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	emptyDigest := mustComputeDigest(t, digestFunction, nil)

	err := validatingCLS.Put(ctx, emptyDigest, makeChunkListBuffer())
	require.NoError(t, err)
}

func TestChunkListValidatingBlobAccessPutRepeatedChunks(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
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

	err := validatingCLS.Put(ctx, expectedDigest, makeChunkListBuffer(digestA, digestA, digestB, digestA))
	require.NoError(t, err)

	composedData, err := fakeCAS.Get(ctx, expectedDigest).ToByteSlice(len(expectedData))
	require.NoError(t, err)
	require.Equal(t, expectedData, composedData)
}

func TestChunkListValidatingBlobAccessPutInlineEmptyChunk(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
	validatingCLS := chunklistvalidating.NewChunkListValidatingBlobAccess(fakeCLS, fakeCAS, maximumMessageSizeBytes)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkData := []byte("Valid")
	chunkDigest := mustComputeDigest(t, digestFunction, chunkData)
	require.NoError(t, fakeCAS.Put(ctx, chunkDigest, buffer.NewValidatedBufferFromByteSlice(chunkData)))

	emptyDigest := mustComputeDigest(t, digestFunction, nil)
	require.NoError(t, fakeCAS.Put(ctx, emptyDigest, buffer.NewValidatedBufferFromByteSlice(nil)))

	expectedDigest := mustComputeDigest(t, digestFunction, chunkData)

	err := validatingCLS.Put(ctx, expectedDigest, makeChunkListBuffer(chunkDigest, emptyDigest))
	require.NoError(t, err)
}

func TestChunkListValidatingBlobAccessPutExtendsLifetimes(t *testing.T) {
	ctx := context.Background()

	fakeCAS := newFakeBlobAccess(testCDCParams)
	fakeCLS := newFakeBlobAccess(nil)
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

	err := validatingCLS.Put(ctx, fullBlobDigest, makeChunkListBuffer(chunk1Digest, chunk2Digest))

	// From the REAPI, the server may either process the splice and
	// return OK, OR it may return ALREADY_EXISTS if the blob is
	// already composed and the server chooses not to extend the
	// lifetime of the user's specific chunks.
	if status.Code(err) == codes.AlreadyExists {
		// The server is free not to touch the user's chunks.
		// However, it MUST still have verified/touched the original
		// blob.
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
