package chunklistvalidating

import (
	"context"
	"io"
	"slices"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkListValidatingBlobAccess struct {
	blobstore.BlobAccess[chunklist.ChunkList]
	zstdPool                zstd.Pool
	cdcParametersFetcher    cdc.ParametersFetcher
	chunkListFetcher        chunklist.Fetcher
	chunkBytesFetcher       reader.Reader[[]byte]
	chunkStorage            blobstore.BlobAccess[*buffer.Chunk]
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
func NewChunkListValidatingBlobAccess(chunkListStorage blobstore.BlobAccess[chunklist.ChunkList], chunkStorage blobstore.BlobAccess[*buffer.Chunk], maximumMessageSizeBytes int, zstdPool zstd.Pool) blobstore.BlobAccess[chunklist.ChunkList] {
	return &chunkListValidatingBlobAccess{
		BlobAccess:              chunkListStorage,
		cdcParametersFetcher:    cdc.NewCapabilitiesParametersFetcher(chunkStorage),
		chunkListFetcher:        blobstore.NewBlobAccessChunkListFetcher(chunkListStorage),
		chunkStorage:            chunkStorage,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		chunkBytesFetcher:       cas.NewChunkBytesReader(chunkStorage),
		zstdPool:                zstdPool,
	}
}

// Get the split result from the downstream blob access, should one
// exist return it only if all its constituent chunks exist.
func (ba *chunkListValidatingBlobAccess) getComplete(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	// Verify the existence of the blob itself against the chunk
	// list storage. This renews the lifetime of the blob even when
	// the chunk list is served from a caching chunk list storage's
	// local cache, because the FindMissing is punched through to
	// the authoritative chunk list storage.
	missing, err := ba.BlobAccess.FindMissing(ctx, d.ToSingletonSet())
	if err != nil || !missing.Empty() {
		return chunklist.ChunkList{}, status.Error(codes.NotFound, "Blob could not be found")
	}

	storedChunkList, err := ba.chunkListFetcher.FetchChunkList(ctx, d)
	if err != nil {
		return chunklist.ChunkList{}, status.Error(codes.NotFound, "Failed to get chunk list")
	}

	digestSetBuilder := digest.NewSetBuilder(len(storedChunkList.Digests))
	for _, digest := range storedChunkList.Digests {
		digestSetBuilder.Add(digest)
	}

	missing, err = ba.chunkStorage.FindMissing(ctx, digestSetBuilder.Build())
	if err == nil && missing.Empty() {
		return storedChunkList, nil
	}
	return chunklist.ChunkList{}, status.Error(codes.NotFound, "Blob could not be found")
}

// Get returns a valid chunk list for the given digest, chunking the
// blob and storing the chunk list if needed.
func (ba *chunkListValidatingBlobAccess) Get(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	params, err := ba.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return chunklist.ChunkList{}, err
	}

	// Check for the trivial case, the blob is small enough that it will
	// always decompose to a single chunk of the same size as the
	// original blob. We verify the existence of the blob in CAS and
	// break out early.
	blobSize := d.GetSizeBytes()
	if blobSize < 2*int64(params.MinChunkSizeBytes) {
		missing, err := ba.chunkStorage.FindMissing(ctx, d.ToSingletonSet())
		if err != nil {
			return chunklist.ChunkList{}, util.StatusWrap(err, "Failed to verify blob existence")
		}
		if !missing.Empty() {
			return chunklist.ChunkList{}, status.Error(codes.NotFound, "Blob not found in CAS")
		}

		chunkList := chunklist.ChunkList{
			Offsets:   []uint64{0},
			Digests:   []digest.Digest{d},
			Validated: true,
		}
		return chunkList, nil
	}

	// Return upstream value if complete.
	return ba.getComplete(ctx, d)
}

// matchesStoredChunkList checks if the user-provided chunk digests
// match the chunk list already stored for the given digest.
func (ba *chunkListValidatingBlobAccess) matchesStoredChunkList(ctx context.Context, d digest.Digest, userChunkList chunklist.ChunkList) (bool, error) {
	storedChunkList, err := ba.BlobAccess.Get(ctx, d)
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, util.StatusWrap(err, "Failed to retrieve stored chunk list")
	}

	return slices.Equal(userChunkList.Digests, storedChunkList.Digests), nil
}

func (ba *chunkListValidatingBlobAccess) Put(ctx context.Context, d digest.Digest, value chunklist.ChunkList) error {
	if value.Validated {
		// ChunkList has already been validated, push it directly to
		// downstream blob store.
		return ba.BlobAccess.Put(ctx, d, value)
	}

	if len(value.Digests) == 0 {
		// Empty chunk list, blob must be the empty blob.
		if d.GetSizeBytes() != 0 {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		if d.GetDigestFunction().NewGenerator(0).Sum() != d {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		return nil
	}

	params, err := ba.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return err
	}

	// Check that all referenced chunks are present in storage.
	value, err = ba.flattenChunks(ctx, params, value)
	if err != nil {
		return status.Error(codes.NotFound, "At least one chunk is missing from storage.")
	}

	// Check if the chunk list is identical to what already exists in
	// storage.
	match, err := ba.matchesStoredChunkList(ctx, d, value)
	if err != nil {
		return err
	}
	if match {
		return nil
	}

	// No more shortcuts available go through the heavy path of
	// concatenating/verifying and chunking the blobs.
	canonicalChunkList := chunklist.ChunkList{
		Offsets:   make([]uint64, 0, len(value.Offsets)),
		Digests:   make([]digest.Digest, 0, len(value.Offsets)),
		Validated: true,
	}
	offset := uint64(0)
	reader := chunklist.NewChunkConcatenatingReader(ctx, value, ba.chunkBytesFetcher)
	digestFunction := d.GetDigestFunction()
	wholeGen := digestFunction.NewGenerator(d.GetSizeBytes())
	chunker := cdc.NewReaderChunker(d.GetDigestFunction(), reader, int64(params.MinChunkSizeBytes), int64(params.HorizonSizeBytes))
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
			if err := ba.chunkStorage.Put(ctx, chunk.Digest, buffer.NewChunk(ba.zstdPool, chunk.Data)); err != nil {
				return util.StatusWrap(err, "Failed to save chunk")
			}
		}
		canonicalChunkList.Offsets = append(canonicalChunkList.Offsets, offset)
		canonicalChunkList.Digests = append(canonicalChunkList.Digests, chunk.Digest)
		offset += uint64(chunk.Digest.GetSizeBytes())
	}

	// Verify the whole blob against the advertised digest.
	if actual := wholeGen.Sum(); actual != d {
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch: advertised %s, actual %s", d, actual)
	}

	// Store the canonical chunk list.
	if err := ba.BlobAccess.Put(ctx, d, canonicalChunkList); err != nil {
		return util.StatusWrap(err, "Failed to save canonical chunk list")
	}
	return nil
}

func (ba *chunkListValidatingBlobAccess) flattenChunks(ctx context.Context, params *remoteexecution.RepMaxCdcParams, userChunkList chunklist.ChunkList) (chunklist.ChunkList, error) {
	maxChunkSize := 2*int64(params.MinChunkSizeBytes) - 1
	bigDigests := digest.NewSetBuilder(len(userChunkList.Digests))
	for _, d := range userChunkList.Digests {
		if d.GetSizeBytes() > maxChunkSize {
			bigDigests.Add(d)
		}
	}
	missing, err := ba.BlobAccess.FindMissing(ctx, bigDigests.Build())
	if err != nil {
		return chunklist.ChunkList{}, util.StatusWrap(err, "Error checking for chunk lists of big chunks")
	}
	if !missing.Empty() {
		return chunklist.ChunkList{}, status.Error(codes.NotFound, "Chunk lists not found for big chunks")
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
				return chunklist.ChunkList{}, util.StatusWrap(err, "Error fetching inner chunk list")
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
		return chunklist.ChunkList{}, util.StatusWrap(err, "Error checking for existence of flattened chunks.")
	}
	if !missing.Empty() {
		return chunklist.ChunkList{}, status.Error(codes.NotFound, "At least one chunk among flattened chunks are missing.")
	}
	return chunklist.ChunkList{
		Offsets:   flattenedOffsets,
		Digests:   flattenedDigests,
		Validated: userChunkList.Validated,
	}, nil
}

func (ba *chunkListValidatingBlobAccess) findMissingChunks(ctx context.Context, d digest.Digest) (digest.Set, error) {
	storedChunkList, err := ba.BlobAccess.Get(ctx, d)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Failed to decode chunk list")
	}
	builder := digest.NewSetBuilder(len(storedChunkList.Digests))
	for _, digest := range storedChunkList.Digests {
		builder.Add(digest)
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
