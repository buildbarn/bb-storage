package chunkmappingvalidating

import (
	"context"
	"slices"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkMappingValidatingBlobAccess struct {
	blobstore.BlobAccess[chunk.Mapping]
	chunkStorage         blobstore.BlobAccess[*chunk.Chunk]
	cdcParametersFetcher capabilities.CDCParametersFetcher
	chunkBytesReader     reader.Reader[[]byte]
	readerPutter         cas.ReaderPutter
}

// NewChunkMappingValidatingBlobAccess creates a wrapper around a Chunk
// Mapping Storage (CMS) that ensures only valid chunk mappings are stored in
// the CMS. A valid chunk mapping is a chunk mapping which follows the
// chunking parameters, has all the chunks present in the Content
// Addressable Storage (CAS) and where the chunks concatenate into the
// appropriate digest.
//
// This validation is fairly expensive and validation should only be
// done at a single layer as close as possible to the CAS where the full
// view of the CAS is available.
func NewChunkMappingValidatingBlobAccess(chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], chunkStorage blobstore.BlobAccess[*chunk.Chunk], cdcParametersFetcher capabilities.CDCParametersFetcher, chunkBytesReader reader.Reader[[]byte], readerPutter cas.ReaderPutter) blobstore.BlobAccess[chunk.Mapping] {
	return &chunkMappingValidatingBlobAccess{
		BlobAccess:           chunkMappingStorage,
		cdcParametersFetcher: cdcParametersFetcher,
		chunkStorage:         chunkStorage,
		chunkBytesReader:     chunkBytesReader,
		readerPutter:         readerPutter,
	}
}

// Get returns a valid chunk mapping for the given digest.
func (ba *chunkMappingValidatingBlobAccess) Get(ctx context.Context, d digest.Digest) (chunk.Mapping, error) {
	// Formally the spec requires us to renew the lifetime of the chunk
	// mapping and its chunks whenever we get it. In practice this would
	// require us to do a FMB on the digest + an FMB on the chunks of
	// the digest to be compliant. But it is unclear why this is
	// required and we hope to remove it from spec.
	//
	// See: https://github.com/bazelbuild/remote-apis/issues/391
	return ba.BlobAccess.Get(ctx, d)
}

// matchesStoredChunkMapping checks if the user-provided chunk digests
// match the chunk mapping already stored for the given digest.
func (ba *chunkMappingValidatingBlobAccess) matchesStoredChunkMapping(ctx context.Context, params *remoteexecution.RepMaxCdcParams, d digest.Digest, userChunkMapping chunk.Mapping) (bool, error) {
	if cas.IsSingleChunk(params, d) {
		// TODO: This can only happen if we are asked to register a
		// chunk mapping for a blob that in the end is a single chunk.
		// Should we prevent this at a higher level?
		return false, nil
	}
	storedChunkMapping, err := ba.BlobAccess.Get(ctx, d)
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, util.StatusWrap(err, "Failed to retrieve stored chunk mapping")
	}
	return slices.Equal(userChunkMapping.GetDigests(), storedChunkMapping.GetDigests()), nil
}

func (ba *chunkMappingValidatingBlobAccess) Put(ctx context.Context, d digest.Digest, value chunk.Mapping) error {
	if value.IsValidated() {
		// ChunkMapping has already been validated, push it directly to
		// downstream blob store.
		return ba.BlobAccess.Put(ctx, d, value)
	}
	params, err := ba.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return err
	}
	// A unvalidated chunk mapping may consist of chunks which canonically
	// are represented by other chunks. We therefore flatten the chunk
	// list and verify all chunks and subchunks are present in the Chunk
	// Storage.
	value, err = ba.flattenAndVerifyPresence(ctx, params, value)
	if err != nil {
		return status.Error(codes.NotFound, "At least one chunk is missing from storage")
	}
	match, err := ba.matchesStoredChunkMapping(ctx, params, d, value)
	if err != nil {
		return err
	}
	if match {
		// The supplied chunk mapping is identical to the one already in
		// storage. We short circuit the verification and skip the rest
		// of the work.
		return nil
	}
	// No more shortcuts available. The blob is reassembled from the
	// stored chunk bytes and re-chunked canonically.
	reader := chunk.NewReaderFromDigests(ctx, value.GetDigests(), ba.chunkBytesReader)
	return ba.readerPutter.PutReader(ctx, d, reader, params)
}

func (ba *chunkMappingValidatingBlobAccess) flattenAndVerifyPresence(ctx context.Context, params *remoteexecution.RepMaxCdcParams, userChunkMapping chunk.Mapping) (chunk.Mapping, error) {
	maxChunkSize := 2*int64(params.MinChunkSizeBytes) - 1
	bigDigests := digest.NewSetBuilder(userChunkMapping.Length())
	for i := range userChunkMapping.Length() {
		d := userChunkMapping.GetDigestAtIndex(i)
		if d.GetSizeBytes() > maxChunkSize {
			bigDigests.Add(d)
		}
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, bigDigests.Build())
	if err != nil {
		return chunk.Mapping{}, util.StatusWrap(err, "Error checking for chunk mappings of big chunks")
	}
	if !missing.Empty() {
		return chunk.Mapping{}, status.Error(codes.NotFound, "Chunk mappings not found for big chunks")
	}
	// An unvalidated chunk mapping may consist of chunks which
	// canonically are represented by other chunks. We therefore flatten
	// the chunk list by expanding the chunk mappings of all big chunks.
	flattenedDigests := make([]digest.Digest, 0, userChunkMapping.Length())
	for i := range userChunkMapping.Length() {
		outerDigest := userChunkMapping.GetDigestAtIndex(i)
		if outerDigest.GetSizeBytes() <= maxChunkSize {
			flattenedDigests = append(flattenedDigests, outerDigest)
		} else {
			innerChunkMapping, err := ba.BlobAccess.Get(ctx, outerDigest)
			if err != nil {
				return chunk.Mapping{}, util.StatusWrap(err, "Error fetching inner chunk mapping")
			}
			flattenedDigests = append(flattenedDigests, innerChunkMapping.GetDigests()...)
		}
	}
	flattenedChunksBuilder := digest.NewSetBuilder(len(flattenedDigests))
	for _, d := range flattenedDigests {
		flattenedChunksBuilder.Add(d)
	}
	missing, err = ba.chunkStorage.FindMissing(ctx, flattenedChunksBuilder.Build())
	if err != nil {
		return chunk.Mapping{}, util.StatusWrap(err, "Error checking for existence of flattened chunks.")
	}
	if !missing.Empty() {
		return chunk.Mapping{}, status.Error(codes.NotFound, "At least one chunk among flattened chunks are missing.")
	}
	// Flattening preserves the total size of the blob, so the flattened
	// mapping is constructed with the same size as the original.
	return chunk.NewMappingFromDigests(flattenedDigests, uint64(userChunkMapping.GetSizeBytes()), userChunkMapping.IsValidated())
}

func (ba *chunkMappingValidatingBlobAccess) findMissingChunksOfMapping(ctx context.Context, d digest.Digest) (digest.Set, error) {
	storedChunkMapping, err := ba.BlobAccess.Get(ctx, d)
	if status.Code(err) == codes.NotFound {
		return digest.EmptySet, status.Error(codes.Internal, "Chunk mapping was missing after explicitly being reported as not missing by FindMissing")
	}
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Failed to fetch chunk mapping")
	}
	builder := digest.NewSetBuilder(storedChunkMapping.Length())
	for i := range storedChunkMapping.Length() {
		builder.Add(storedChunkMapping.GetDigestAtIndex(i))
	}
	// TODO: This causes one find missing call per chunk mapping but we
	// could aggregate these into a single call with a bit of
	// bookkeeping.
	return ba.chunkStorage.FindMissing(ctx, builder.Build())
}

func (ba *chunkMappingValidatingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missingBlobs, err := ba.BlobAccess.FindMissing(ctx, digests)
	if err != nil {
		return digest.EmptySet, err
	}
	nonMissingBlobs, _, _ := digest.GetDifferenceAndIntersection(digests, missingBlobs)
	missings := make([]digest.Set, 1, 1+nonMissingBlobs.Length())
	missings[0] = missingBlobs
	for _, d := range nonMissingBlobs.Items() {
		missingChunks, err := ba.findMissingChunksOfMapping(ctx, d)
		if err != nil {
			return digest.EmptySet, err
		}
		if !missingChunks.Empty() {
			missings = append(missings, d.ToSingletonSet())
		}
	}
	return digest.GetUnion(missings), nil
}
