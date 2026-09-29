package chunkmappingvalidating

import (
	"context"
	"io"
	"slices"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkMappingValidatingBlobAccess struct {
	blobstore.BlobAccess[chunk.Mapping]
	zstdPool                zstd.Pool
	cdcParametersFetcher    capabilities.CDCParametersFetcher
	chunkMappingFetcher     chunk.MappingFetcher
	chunkBytesFetcher       reader.Reader[[]byte]
	chunkStorage            blobstore.BlobAccess[*chunk.Chunk]
	maximumMessageSizeBytes int
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
func NewChunkMappingValidatingBlobAccess(chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], chunkStorage blobstore.BlobAccess[*chunk.Chunk], maximumMessageSizeBytes int, zstdPool zstd.Pool) blobstore.BlobAccess[chunk.Mapping] {
	return &chunkMappingValidatingBlobAccess{
		BlobAccess:              chunkMappingStorage,
		cdcParametersFetcher:    capabilities.NewCDCParametersFetcher(chunkStorage),
		chunkMappingFetcher:     blobstore.NewBlobAccessMappingFetcher(chunkMappingStorage),
		chunkStorage:            chunkStorage,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		chunkBytesFetcher:       cas.NewChunkBytesReader(chunkStorage),
		zstdPool:                zstdPool,
	}
}

// Get returns a valid chunk mapping for the given digest.
func (ba *chunkMappingValidatingBlobAccess) Get(ctx context.Context, d digest.Digest) (chunk.Mapping, error) {
	// Verify the existence of the blob itself against the chunk
	// list storage. This renews the lifetime of the blob even when
	// the chunk mapping is served from a caching chunk mapping storage's
	// local cache, because the FindMissing is punched through to
	// the authoritative chunk mapping storage.
	missing, err := ba.BlobAccess.FindMissing(ctx, d.ToSingletonSet())
	if err != nil {
		return chunk.Mapping{}, util.StatusWrap(err, "Failed to check for blob existence")
	}
	if !missing.Empty() {
		return chunk.Mapping{}, status.Error(codes.NotFound, "Blob could not be found")
	}
	storedChunkMapping, err := ba.chunkMappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return chunk.Mapping{}, util.StatusWrap(err, "Failed to get chunk mapping")
	}
	digestSetBuilder := digest.NewSetBuilder(len(storedChunkMapping.Digests))
	for _, digest := range storedChunkMapping.Digests {
		digestSetBuilder.Add(digest)
	}
	missing, err = ba.chunkStorage.FindMissing(ctx, digestSetBuilder.Build())
	if err != nil {
		return chunk.Mapping{}, util.StatusWrap(err, "Failed to check for chunk existence")
	}
	if !missing.Empty() {
		return chunk.Mapping{}, status.Error(codes.NotFound, "Blob could not be found")
	}
	return storedChunkMapping, nil
}

// matchesStoredChunkMapping checks if the user-provided chunk digests
// match the chunk mapping already stored for the given digest.
func (ba *chunkMappingValidatingBlobAccess) matchesStoredChunkMapping(ctx context.Context, d digest.Digest, userChunkMapping chunk.Mapping) (bool, error) {
	storedChunkMapping, err := ba.BlobAccess.Get(ctx, d)
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, util.StatusWrap(err, "Failed to retrieve stored chunk mapping")
	}
	return slices.Equal(userChunkMapping.Digests, storedChunkMapping.Digests), nil
}

func (ba *chunkMappingValidatingBlobAccess) Put(ctx context.Context, d digest.Digest, value chunk.Mapping) error {
	if value.Validated {
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
	match, err := ba.matchesStoredChunkMapping(ctx, d, value)
	if err != nil {
		return err
	}
	if match {
		// The supplied chunk mapping is identical to the one already in
		// storage. We short circuit the verification and skip the rest
		// of the work.
		return nil
	}
	// No more shortcuts available go through the heavy path of
	// concatenating/verifying and chunking the blobs.
	canonicalDigests := make([]digest.Digest, 0, len(value.Offsets))
	offset := uint64(0)
	reader := chunk.NewReaderFromMapping(ctx, value.Digests, ba.chunkBytesFetcher)
	digestFunction := d.GetDigestFunction()
	wholeGen := digestFunction.NewGenerator(d.GetSizeBytes())
	chunker := cdc.NewReaderChunker(d.GetDigestFunction(), reader, int64(params.MinChunkSizeBytes), int64(params.HorizonSizeBytes))
	for {
		c, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := wholeGen.Write(c.Data); err != nil {
			return status.Error(codes.Internal, "Could not compute digest of blob")
		}
		missing, err := ba.chunkStorage.FindMissing(ctx, c.Digest.ToSingletonSet())
		if err != nil {
			return err
		}
		if !missing.Empty() {
			// The calculated chunk was not present in Chunk Storage.
			if err := ba.chunkStorage.Put(ctx, c.Digest, chunk.NewChunk(ba.zstdPool, c.Data)); err != nil {
				return util.StatusWrap(err, "Failed to save chunk")
			}
		}
		canonicalDigests = append(canonicalDigests, c.Digest)
		offset += uint64(c.Digest.GetSizeBytes())
	}
	if actual := wholeGen.Sum(); actual != d {
		// The chunks in the supplied chunkmapping do not add up to its
		// digest.
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, advertised %s, actual %s", d, actual)
	}
	if len(canonicalDigests) < 2 {
		// Blobs that fit in a single chunk have no chunk mappings in
		// storage, as the blob is stored as the chunk itself.
		return nil
	}
	// Store the canonical chunk mapping.
	canonicalChunkMapping, err := chunk.NewMapping(canonicalDigests, uint64(d.GetSizeBytes()), true)
	if err != nil {
		return err
	}
	if err := ba.BlobAccess.Put(ctx, d, canonicalChunkMapping); err != nil {
		return util.StatusWrap(err, "Failed to save canonical chunk mapping")
	}
	return nil
}

func (ba *chunkMappingValidatingBlobAccess) flattenAndVerifyPresence(ctx context.Context, params *remoteexecution.RepMaxCdcParams, userChunkMapping chunk.Mapping) (chunk.Mapping, error) {
	maxChunkSize := 2*int64(params.MinChunkSizeBytes) - 1
	bigDigests := digest.NewSetBuilder(len(userChunkMapping.Digests))
	for _, d := range userChunkMapping.Digests {
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
	flattenedOffsets := make([]uint64, 0, len(userChunkMapping.Offsets))
	flattenedDigests := make([]digest.Digest, 0, len(userChunkMapping.Digests))
	flattenedChunksBuilder := digest.NewSetBuilder(len(userChunkMapping.Digests))
	for i, outerDigest := range userChunkMapping.Digests {
		outerOffset := userChunkMapping.Offsets[i]
		if outerDigest.GetSizeBytes() <= maxChunkSize {
			flattenedOffsets = append(flattenedOffsets, outerOffset)
			flattenedDigests = append(flattenedDigests, outerDigest)
			flattenedChunksBuilder.Add(outerDigest)
		} else {
			innerChunkMapping, err := ba.chunkMappingFetcher.FetchChunkMapping(ctx, outerDigest)
			if err != nil {
				return chunk.Mapping{}, util.StatusWrap(err, "Error fetching inner chunk mapping")
			}
			for j, innerDigest := range innerChunkMapping.Digests {
				innerOffset := innerChunkMapping.Offsets[j]
				flattenedOffsets = append(flattenedOffsets, outerOffset+innerOffset)
				flattenedDigests = append(flattenedDigests, innerDigest)
				flattenedChunksBuilder.Add(innerDigest)
			}
		}
	}
	missing, err = ba.chunkStorage.FindMissing(ctx, flattenedChunksBuilder.Build())
	if err != nil {
		return chunk.Mapping{}, util.StatusWrap(err, "Error checking for existence of flattened chunks.")
	}
	if !missing.Empty() {
		return chunk.Mapping{}, status.Error(codes.NotFound, "At least one chunk among flattened chunks are missing.")
	}
	return chunk.Mapping{
		Offsets:   flattenedOffsets,
		Digests:   flattenedDigests,
		Validated: userChunkMapping.Validated,
	}, nil
}

func (ba *chunkMappingValidatingBlobAccess) findMissingChunksOfMapping(ctx context.Context, d digest.Digest) (digest.Set, error) {
	storedChunkMapping, err := ba.chunkMappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Failed to fetch chunk mapping")
	}
	builder := digest.NewSetBuilder(len(storedChunkMapping.Digests))
	for _, digest := range storedChunkMapping.Digests {
		builder.Add(digest)
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
