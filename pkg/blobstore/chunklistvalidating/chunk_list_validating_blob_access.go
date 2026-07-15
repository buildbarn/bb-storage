package chunklistvalidating

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkListValidatingBlobAccess struct {
	blobstore.BlobAccess
	chunkStorage            blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewChunkListValidatingBlobAccess creates a wrapper around a Chunk
// List Storage (CLS) that ensures only valid chunk lists are stored in
// the CLS. A valid chunk list is a chunk list which follows the
// chunking parameters, has all the chunks present in the Content
// Addressable Storage (CAS) and where the chunks concatenate into the
// appropriate digest.
//
// This validation is fairly expensive and validation should only be
// done at a single layer as close as possible to the CAS where the full
// view of the CAS is available.
func NewChunkListValidatingBlobAccess(chunkListStorage, chunkStorage blobstore.BlobAccess, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &chunkListValidatingBlobAccess{
		BlobAccess:              chunkListStorage,
		chunkStorage:            chunkStorage,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

// Get the split result from the downstream blob access, should one
// exist return it only if all its constituent chunks exist.
func (ba *chunkListValidatingBlobAccess) getComplete(ctx context.Context, d digest.Digest) buffer.Buffer {
	missing, err := ba.BlobAccess.FindMissing(ctx, d.ToSingletonSet())
	if err != nil || !missing.Empty() {
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob could not be found."))
	}
	b1, b2 := ba.BlobAccess.Get(ctx, d).CloneCopy(ba.maximumMessageSizeBytes)
	responseMsg, err := b1.ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		b2.Discard()
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Failed to parse chunk list."))
	}

	splitBlobResponse := responseMsg.(*remoteexecution.SplitBlobResponse)
	digestFunction := d.GetDigestFunction()
	digestSetBuilder := digest.NewSetBuilder(len(splitBlobResponse.ChunkDigests))

	for _, chunkDigestProto := range splitBlobResponse.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			b2.Discard()
			return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to parse digest of chunk."))
		}
		digestSetBuilder.Add(chunkDigest)
	}

	missing, err = ba.chunkStorage.FindMissing(ctx, digestSetBuilder.Build())
	if err == nil && missing.Empty() {
		return b2
	}
	b2.Discard()
	return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob could not be found."))
}

// Get returns a valid SplitResult for the given digest chunking the
// blob and storing the chunk list if needed.
func (ba *chunkListValidatingBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	params, err := cdc.GetCDCParameters(ctx, ba, d.GetInstanceName())
	if err != nil {
		return buffer.NewBufferFromError(err)
	}

	// Check for the trivial case, the blob is small enough that it will
	// always decompose to a single chunk of the same size as the
	// original blob. We verify the existence of the blob in CAS and
	// break out early.
	blobSize := d.GetSizeBytes()
	if blobSize < 2*params.MinChunkSizeBytes {
		missing, err := ba.chunkStorage.FindMissing(ctx, d.ToSingletonSet())
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to verify blob existence"))
		}
		if !missing.Empty() {
			return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found in CAS"))
		}

		response := &remoteexecution.SplitBlobResponse{
			ChunkDigests:     []*remoteexecution.Digest{d.GetProto()},
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}

		return buffer.NewProtoBufferFromProto(response, buffer.UserProvided)
	}

	// Return upstream split result if complete.
	return ba.getComplete(ctx, d)
}

func (ba *chunkListValidatingBlobAccess) matchesStoredChunkList(ctx context.Context, d digest.Digest, userResponse *remoteexecution.SplitBlobResponse) bool {
	existingMsg, err := ba.BlobAccess.Get(ctx, d).ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return false
	}

	cachedResponse := existingMsg.(*remoteexecution.SplitBlobResponse)
	if len(cachedResponse.ChunkDigests) == len(userResponse.ChunkDigests) {
		for i, c := range cachedResponse.ChunkDigests {
			u := userResponse.ChunkDigests[i]
			if u.Hash != c.Hash || u.SizeBytes != c.SizeBytes {
				return false
			}
		}
	}

	return true
}

func (ba *chunkListValidatingBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	// Parse the buffer as a SplitBlobResponse
	msg, err := b.ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return util.StatusWrap(err, "Failed to parse input as SplitBlobResponse")
	}
	inResponse := msg.(*remoteexecution.SplitBlobResponse)

	params, err := cdc.GetCDCParameters(ctx, ba, d.GetInstanceName())
	if err != nil {
		return err
	}

	digestFunction := d.GetDigestFunction()
	var userChunks []digest.Digest
	for _, chunkDigestProto := range inResponse.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Invalid chunk digest: %v", err)
		}
		userChunks = append(userChunks, chunkDigest)
	}

	// No chunks given, blob must be the empty blob.
	if len(userChunks) == 0 {
		if d.GetSizeBytes() != 0 {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		if d.GetDigestFunction().NewGenerator(0).Sum() != d {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		return nil
	}

	// Check that all referenced chunks are present in storage.
	userChunks, err = ba.flattenChunks(ctx, params, userChunks)
	if err != nil {
		return status.Error(codes.NotFound, "At least one chunk is missing from storage.")
	}

	// Chunk list is marked for validation bypass, push it directy to
	// downstream blob store.
	if cdc.ChunkListValidationBypassed(ctx) {
		return ba.BlobAccess.Put(ctx, d, b)
	}

	// Check the trivial case without hitting the downstream blob
	// stores.

	if ba.matchesStoredChunkList(ctx, d, inResponse) {
		return nil
	}

	// No more shortcuts available go through the heavy path of
	// concatenating/verifying and chunking the blobs.
	blobBuffer := buffer.NewUnvalidatedCASChunkConcatenatingBuffer(ctx, d, userChunks, ba.chunkStorage.Get, buffer.UserProvided, ba.maximumMessageSizeBytes)
	var canonicalChunkDigests []*remoteexecution.Digest
	reader := blobBuffer.ToReader()
	defer reader.Close()
	chunker := cdc.NewReaderChunker(d.GetDigestFunction(), reader, int64(params.MinChunkSizeBytes), int64(params.HorizonSizeBytes))
	for {
		chunk, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		missing, err := ba.chunkStorage.FindMissing(ctx, chunk.Digest.ToSingletonSet())
		if err != nil {
			return err
		}
		if !missing.Empty() {
			if err := ba.chunkStorage.Put(ctx, chunk.Digest, buffer.NewValidatedBufferFromByteSlice(chunk.Data)); err != nil {
				return util.StatusWrap(err, "Failed to save chunk")
			}
		}
		canonicalChunkDigests = append(canonicalChunkDigests, chunk.Digest.GetProto())
	}

	// Store the canonical response.
	canonicalResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests:     canonicalChunkDigests,
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}
	canonicalBuffer := buffer.NewProtoBufferFromProto(canonicalResponse, buffer.UserProvided)
	if err := ba.BlobAccess.Put(ctx, d, canonicalBuffer); err != nil {
		return util.StatusWrap(err, "Failed to save canonical chunk list")
	}
	return nil
}

func (ba *chunkListValidatingBlobAccess) flattenChunks(ctx context.Context, params cdc.Parameters, userChunks []digest.Digest) ([]digest.Digest, error) {
	maxChunkSize := int64(2*params.MinChunkSizeBytes - 1)
	bigDigests := digest.NewSetBuilder(len(userChunks))
	for _, chunkDigest := range userChunks {
		if chunkDigest.GetSizeBytes() > maxChunkSize {
			bigDigests.Add(chunkDigest)
		}
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, bigDigests.Build())
	if err != nil {
		return nil, util.StatusWrap(err, "Error checking for chunk lists of big chunks")
	}
	if !missing.Empty() {
		return nil, status.Error(codes.NotFound, "Chunk lists not found for big chunks.")
	}
	flattenedChunks := make([]digest.Digest, 0, len(userChunks))
	flattenedChunksBuilder := digest.NewSetBuilder(len(userChunks))
	for _, chunkDigest := range userChunks {
		digestFunction := chunkDigest.GetDigestFunction()
		if chunkDigest.GetSizeBytes() <= maxChunkSize {
			flattenedChunks = append(flattenedChunks, chunkDigest)
			flattenedChunksBuilder.Add(chunkDigest)
		} else {
			innerChunksResponseBuffer := ba.BlobAccess.Get(ctx, chunkDigest)
			innerChunksResponseProtoBuf, err := innerChunksResponseBuffer.ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
			if err != nil {
				return nil, util.StatusWrap(err, "Error reading chunk list for big chunk")
			}
			innerChunksResponseProto := innerChunksResponseProtoBuf.(*remoteexecution.SplitBlobResponse)
			for _, innerChunkDigestProto := range innerChunksResponseProto.ChunkDigests {
				innerDigest, err := digestFunction.NewDigestFromProto(innerChunkDigestProto)
				if err != nil {
					return nil, util.StatusWrap(err, "Error parsing digest of chunk list of big chunk")
				}
				flattenedChunks = append(flattenedChunks, innerDigest)
				flattenedChunksBuilder.Add(innerDigest)
			}
		}
	}
	missing, err = ba.chunkStorage.FindMissing(ctx, flattenedChunksBuilder.Build())
	if err != nil {
		return nil, util.StatusWrap(err, "Error checking for existence of flattened chunks.")
	}
	if !missing.Empty() {
		return nil, status.Error(codes.NotFound, "At least one chunk among flattened chunks are missing.")
	}
	return flattenedChunks, nil
}

func (ba *chunkListValidatingBlobAccess) findMissingChunks(ctx context.Context, d digest.Digest) (digest.Set, error) {
	splitBlobResponseProto, err := ba.BlobAccess.Get(ctx, d).ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return digest.EmptySet, err
	}
	splitBlobResponse := splitBlobResponseProto.(*remoteexecution.SplitBlobResponse)
	digestFunction := d.GetDigestFunction()
	builder := digest.NewSetBuilder(len(splitBlobResponse.ChunkDigests))
	for _, chunkDigestProto := range splitBlobResponse.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Invalid chunk digest %#v", chunkDigestProto)
		}
		builder.Add(chunkDigest)
	}
	return ba.chunkStorage.FindMissing(ctx, builder.Build())
}

func (ba *chunkListValidatingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missingBlobs, err := ba.BlobAccess.FindMissing(ctx, digests)
	if err != nil {
		return digest.EmptySet, err
	}
	nonMissingBlobs, _, _ := digest.GetDifferenceAndIntersection(digests, missingBlobs)
	missings := make([]digest.Set, 1, 1+nonMissingBlobs.Length())
	missings[0] = missingBlobs
	for _, d := range nonMissingBlobs.Items() {
		missingChunks, err := ba.findMissingChunks(ctx, d)
		if err != nil {
			return digest.EmptySet, err
		}
		if !missingChunks.Empty() {
			missings = append(missings, d.ToSingletonSet())
		}
	}
	return digest.GetUnion(missings), nil
}
