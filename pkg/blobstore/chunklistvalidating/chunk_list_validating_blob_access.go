package chunklistvalidating

import (
	"context"
	"io"
	"slices"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	chunklist_pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkListValidatingBlobAccess struct {
	blobstore.BlobAccess
	cdcParametersFetcher    cdc.ParametersFetcher
	chunkListFetcher        chunklist.Fetcher
	chunkFetcher            chunklist.ChunkFetcher
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
		cdcParametersFetcher:    cdc.NewCapabilitiesParametersFetcher(chunkStorage),
		chunkListFetcher:        blobstore.NewBlobAccessChunkListFetcher(chunkListStorage, maximumMessageSizeBytes),
		chunkStorage:            chunkStorage,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		chunkFetcher: chunklist.NewChunkFetcherFromFunction(func(ctx context.Context, digest digest.Digest) ([]byte, error) {
			return chunkStorage.Get(ctx, digest).ToByteSlice(maximumMessageSizeBytes)
		}),
	}
}

// Get the split result from the downstream blob access, should one
// exist return it only if all its constituent chunks exist.
func (ba *chunkListValidatingBlobAccess) getComplete(ctx context.Context, d digest.Digest) buffer.Buffer {
	// Verify the existence of the blob itself against the chunk
	// list storage. This renews the lifetime of the blob even when
	// the chunk list is served from a caching chunk list storage's
	// local cache, because the FindMissing is punched through to
	// the authoritative chunk list storage.
	missing, err := ba.BlobAccess.FindMissing(ctx, d.ToSingletonSet())
	if err != nil || !missing.Empty() {
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob could not be found"))
	}

	storedChunkList, err := ba.chunkListFetcher.FetchChunkList(ctx, d)
	if err != nil {
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Failed to get chunk list"))
	}

	digestSetBuilder := digest.NewSetBuilder(len(storedChunkList))
	for _, entry := range storedChunkList {
		digestSetBuilder.Add(entry.Digest)
	}

	missing, err = ba.chunkStorage.FindMissing(ctx, digestSetBuilder.Build())
	if err == nil && missing.Empty() {
		return buffer.NewProtoBufferFromProto(chunklist.ToProto(storedChunkList), buffer.UserProvided)
	}
	return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob could not be found"))
}

// Get returns a valid chunk list for the given digest, chunking the
// blob and storing the chunk list if needed.
func (ba *chunkListValidatingBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	params, err := ba.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
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

		chunkList := chunklist.ChunkList{
			{Offset: 0, Digest: d},
		}
		return buffer.NewProtoBufferFromProto(chunklist.ToProto(chunkList), buffer.UserProvided)
	}

	// Return upstream split result if complete.
	return ba.getComplete(ctx, d)
}

// matchesStoredChunkList checks if the user-provided chunk digests
// match the chunk list already stored for the given digest.
func (ba *chunkListValidatingBlobAccess) matchesStoredChunkList(ctx context.Context, d digest.Digest, userChunkList chunklist.ChunkList) bool {
	msg, err := ba.BlobAccess.Get(ctx, d).ToProto(&chunklist_pb.ChunkList{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return false
	}

	storedChunkList, err := chunklist.NewChunkListFromProto(msg.(*chunklist_pb.ChunkList), d.GetInstanceName())
	if err != nil {
		return false
	}

	return slices.Equal(userChunkList, storedChunkList)
}

func (ba *chunkListValidatingBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	// Parse the buffer as a ChunkList proto.
	msg, err := b.ToProto(&chunklist_pb.ChunkList{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return util.StatusWrap(err, "Failed to parse input as ChunkList")
	}
	userChunkList, err := chunklist.NewChunkListFromProto(msg.(*chunklist_pb.ChunkList), d.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Failed to decode chunk list")
	}

	params, err := ba.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return err
	}

	// No chunks given, blob must be the empty blob.
	if len(userChunkList) == 0 {
		if d.GetSizeBytes() != 0 {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		if d.GetDigestFunction().NewGenerator(0).Sum() != d {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		return nil
	}

	// Check that all referenced chunks are present in storage.
	userChunkList, err = ba.flattenChunks(ctx, params, userChunkList)
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

	if ba.matchesStoredChunkList(ctx, d, userChunkList) {
		return nil
	}

	// No more shortcuts available go through the heavy path of
	// concatenating/verifying and chunking the blobs.
	canonicalChunkList := make(chunklist.ChunkList, 0, len(userChunkList))
	offset := uint64(0)
	reader := chunklist.NewChunkConcatenatingReader(ctx, userChunkList, ba.chunkFetcher)
	digestFunction := d.GetDigestFunction()
	wholeGen := digestFunction.NewGenerator(d.GetSizeBytes())
	chunker := cdc.NewReaderChunker(d.GetDigestFunction(), reader, params.MinChunkSizeBytes, params.HorizonSizeBytes)
	for {
		chunk, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if _, err := wholeGen.Write(chunk.Data); err != nil {
			return status.Error(codes.Internal, "Could not compute digest of blob")
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
		canonicalChunkList = append(canonicalChunkList, chunklist.Entry{Digest: chunk.Digest, Offset: offset})
		offset += uint64(chunk.Digest.GetSizeBytes())
	}

	// Verify the whole blob against the advertised digest.
	if actual := wholeGen.Sum(); actual != d {
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch: advertised %s, actual %s", d, actual)
	}

	// Store the canonical chunk list.
	canonicalBuffer := buffer.NewProtoBufferFromProto(chunklist.ToProto(canonicalChunkList), buffer.UserProvided)
	if err := ba.BlobAccess.Put(ctx, d, canonicalBuffer); err != nil {
		return util.StatusWrap(err, "Failed to save canonical chunk list")
	}
	return nil
}

func (ba *chunkListValidatingBlobAccess) flattenChunks(ctx context.Context, params cdc.Parameters, userChunkList chunklist.ChunkList) (chunklist.ChunkList, error) {
	maxChunkSize := int64(2*params.MinChunkSizeBytes - 1)
	bigDigests := digest.NewSetBuilder(len(userChunkList))
	for _, entry := range userChunkList {
		if entry.Digest.GetSizeBytes() > maxChunkSize {
			bigDigests.Add(entry.Digest)
		}
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, bigDigests.Build())
	if err != nil {
		return nil, util.StatusWrap(err, "Error checking for chunk lists of big chunks")
	}
	if !missing.Empty() {
		return nil, status.Error(codes.NotFound, "Chunk lists not found for big chunks")
	}
	flattenedChunkList := make(chunklist.ChunkList, 0, len(userChunkList))
	flattenedChunksBuilder := digest.NewSetBuilder(len(userChunkList))
	for _, outerEntry := range userChunkList {
		outerDigest := outerEntry.Digest
		if outerDigest.GetSizeBytes() <= maxChunkSize {
			flattenedChunkList = append(flattenedChunkList, outerEntry)
			flattenedChunksBuilder.Add(outerDigest)
		} else {
			innerChunkList, err := ba.chunkListFetcher.FetchChunkList(ctx, outerDigest)
			if err != nil {
				return nil, util.StatusWrap(err, "Error fetching inner chunk list")
			}
			for _, innerChunkEntry := range innerChunkList {
				innerEntry := chunklist.Entry{
					Digest: innerChunkEntry.Digest,
					Offset: innerChunkEntry.Offset + outerEntry.Offset,
				}
				flattenedChunkList = append(flattenedChunkList, innerEntry)
				flattenedChunksBuilder.Add(innerEntry.Digest)
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
	return flattenedChunkList, nil
}

func (ba *chunkListValidatingBlobAccess) findMissingChunks(ctx context.Context, d digest.Digest) (digest.Set, error) {
	msg, err := ba.BlobAccess.Get(ctx, d).ToProto(&chunklist_pb.ChunkList{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return digest.EmptySet, err
	}
	storedChunkList, err := chunklist.NewChunkListFromProto(msg.(*chunklist_pb.ChunkList), d.GetInstanceName())
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Failed to decode chunk list")
	}
	builder := digest.NewSetBuilder(len(storedChunkList))
	for _, entry := range storedChunkList {
		builder.Add(entry.Digest)
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
