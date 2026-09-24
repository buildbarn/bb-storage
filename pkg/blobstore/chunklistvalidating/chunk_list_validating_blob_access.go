package chunklistvalidating

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

type chunkListValidatingBlobAccess struct {
	blobstore.BlobAccess[chunk.List]
	zstdPool                zstd.Pool
	cdcParametersFetcher    capabilities.CDCParametersFetcher
	chunkListFetcher        chunk.ListFetcher
	chunkBytesFetcher       reader.Reader[[]byte]
	chunkStorage            blobstore.BlobAccess[*chunk.Chunk]
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
func NewChunkListValidatingBlobAccess(chunkListStorage blobstore.BlobAccess[chunk.List], chunkStorage blobstore.BlobAccess[*chunk.Chunk], maximumMessageSizeBytes int, zstdPool zstd.Pool) blobstore.BlobAccess[chunk.List] {
	return &chunkListValidatingBlobAccess{
		BlobAccess:              chunkListStorage,
		cdcParametersFetcher:    capabilities.NewCDCParametersFetcher(chunkStorage),
		chunkListFetcher:        blobstore.NewBlobAccessChunkListFetcher(chunkListStorage),
		chunkStorage:            chunkStorage,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		chunkBytesFetcher:       cas.NewChunkBytesReader(chunkStorage),
		zstdPool:                zstdPool,
	}
}

// Get returns a valid chunk list for the given digest.
func (ba *chunkListValidatingBlobAccess) Get(ctx context.Context, d digest.Digest) (chunk.List, error) {
	// Verify the existence of the blob itself against the chunk
	// list storage. This renews the lifetime of the blob even when
	// the chunk list is served from a caching chunk list storage's
	// local cache, because the FindMissing is punched through to
	// the authoritative chunk list storage.
	missing, err := ba.BlobAccess.FindMissing(ctx, d.ToSingletonSet())
	if err != nil {
		return chunk.List{}, util.StatusWrap(err, "Failed to check for blob existence")
	}
	if !missing.Empty() {
		return chunk.List{}, status.Error(codes.NotFound, "Blob could not be found")
	}
	storedChunkList, err := ba.chunkListFetcher.FetchChunkList(ctx, d)
	if err != nil {
		return chunk.List{}, util.StatusWrap(err, "Failed to get chunk list")
	}
	digestSetBuilder := digest.NewSetBuilder(len(storedChunkList.Digests))
	for _, digest := range storedChunkList.Digests {
		digestSetBuilder.Add(digest)
	}
	missing, err = ba.chunkStorage.FindMissing(ctx, digestSetBuilder.Build())
	if err != nil {
		return chunk.List{}, util.StatusWrap(err, "Failed to check for chunk existence")
	}
	if !missing.Empty() {
		return chunk.List{}, status.Error(codes.NotFound, "Blob could not be found")
	}
	return storedChunkList, nil
}

// matchesStoredChunkList checks if the user-provided chunk digests
// match the chunk list already stored for the given digest.
func (ba *chunkListValidatingBlobAccess) matchesStoredChunkList(ctx context.Context, d digest.Digest, userChunkList chunk.List) (bool, error) {
	storedChunkList, err := ba.BlobAccess.Get(ctx, d)
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, util.StatusWrap(err, "Failed to retrieve stored chunk list")
	}
	return slices.Equal(userChunkList.Digests, storedChunkList.Digests), nil
}

func (ba *chunkListValidatingBlobAccess) Put(ctx context.Context, d digest.Digest, value chunk.List) error {
	if value.Validated {
		// ChunkList has already been validated, push it directly to
		// downstream blob store.
		return ba.BlobAccess.Put(ctx, d, value)
	}
	params, err := ba.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return err
	}
	// A unvalidated chunk list may consist of chunks which canonically
	// are represented by other chunks. We therefore flatten the chunk
	// list and verify all chunks and subchunks are present in the Chunk
	// Storage.
	value, err = ba.flattenAndVerifyPresence(ctx, params, value)
	if err != nil {
		return status.Error(codes.NotFound, "At least one chunk is missing from storage")
	}
	match, err := ba.matchesStoredChunkList(ctx, d, value)
	if err != nil {
		return err
	}
	if match {
		// The supplied chunk list is identical to the one already in
		// storage. We short circuit the verification and skip the rest
		// of the work.
		return nil
	}
	// No more shortcuts available go through the heavy path of
	// concatenating/verifying and chunking the blobs.
	canonicalChunkList := chunk.List{
		Offsets:   make([]uint64, 0, len(value.Offsets)),
		Digests:   make([]digest.Digest, 0, len(value.Offsets)),
		Validated: true,
	}
	offset := uint64(0)
	reader := chunk.NewReaderFromList(ctx, value, ba.chunkBytesFetcher)
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
		canonicalChunkList.Offsets = append(canonicalChunkList.Offsets, offset)
		canonicalChunkList.Digests = append(canonicalChunkList.Digests, c.Digest)
		offset += uint64(c.Digest.GetSizeBytes())
	}
	if actual := wholeGen.Sum(); actual != d {
		// The chunks in the supplied chunklist do not add up to its
		// digest.
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch: advertised %s, actual %s", d, actual)
	}
	// Store the canonical chunk list.
	if err := ba.BlobAccess.Put(ctx, d, canonicalChunkList); err != nil {
		return util.StatusWrap(err, "Failed to save canonical chunk list")
	}
	return nil
}

func (ba *chunkListValidatingBlobAccess) flattenAndVerifyPresence(ctx context.Context, params *remoteexecution.RepMaxCdcParams, userChunkList chunk.List) (chunk.List, error) {
	maxChunkSize := 2*int64(params.MinChunkSizeBytes) - 1
	bigDigests := digest.NewSetBuilder(len(userChunkList.Digests))
	for _, d := range userChunkList.Digests {
		if d.GetSizeBytes() > maxChunkSize {
			bigDigests.Add(d)
		}
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, bigDigests.Build())
	if err != nil {
		return chunk.List{}, util.StatusWrap(err, "Error checking for chunk lists of big chunks")
	}
	if !missing.Empty() {
		return chunk.List{}, status.Error(codes.NotFound, "Chunk lists not found for big chunks")
	}
	flattenedOffsets := make([]uint64, 0, len(userChunkList.Offsets))
	flattenedDigests := make([]digest.Digest, 0, len(userChunkList.Digests))
	flattenedChunksBuilder := digest.NewSetBuilder(len(userChunkList.Digests))
	for i, outerDigest := range userChunkList.Digests {
		outerOffset := userChunkList.Offsets[i]
		if outerDigest.GetSizeBytes() <= maxChunkSize {
			flattenedOffsets = append(flattenedOffsets, outerOffset)
			flattenedDigests = append(flattenedDigests, outerDigest)
			flattenedChunksBuilder.Add(outerDigest)
		} else {
			innerChunkList, err := ba.chunkListFetcher.FetchChunkList(ctx, outerDigest)
			if err != nil {
				return chunk.List{}, util.StatusWrap(err, "Error fetching inner chunk list")
			}
			for j, innerDigest := range innerChunkList.Digests {
				innerOffset := innerChunkList.Offsets[j]
				flattenedOffsets = append(flattenedOffsets, outerOffset+innerOffset)
				flattenedDigests = append(flattenedDigests, innerDigest)
				flattenedChunksBuilder.Add(innerDigest)
			}
		}
	}
	missing, err = ba.chunkStorage.FindMissing(ctx, flattenedChunksBuilder.Build())
	if err != nil {
		return chunk.List{}, util.StatusWrap(err, "Error checking for existence of flattened chunks.")
	}
	if !missing.Empty() {
		return chunk.List{}, status.Error(codes.NotFound, "At least one chunk among flattened chunks are missing.")
	}
	return chunk.List{
		Offsets:   flattenedOffsets,
		Digests:   flattenedDigests,
		Validated: userChunkList.Validated,
	}, nil
}

func (ba *chunkListValidatingBlobAccess) findMissingChunks(ctx context.Context, d digest.Digest) (digest.Set, error) {
	storedChunkList, err := ba.chunkListFetcher.FetchChunkList(ctx, d)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Failed to fetch chunk list")
	}
	builder := digest.NewSetBuilder(len(storedChunkList.Digests))
	for _, digest := range storedChunkList.Digests {
		builder.Add(digest)
	}
	// TODO: This causes one find missing call per chunk list but we
	// could aggregate these into a single call with a bit of
	// bookkeeping.
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
