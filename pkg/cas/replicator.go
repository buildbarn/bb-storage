package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
)

// Replicator replicates blobs from a source Content Addressable Storage
// (CAS) to a sink CAS.
type Replicator interface {
	// Replicate blobs from the set of digests.
	Replicate(ctx context.Context, digests digest.Set) error
}

type replicator struct {
	zstdPool               zstd.Pool
	sourceChunkBytesReader reader.Reader[[]byte]
	sourceChunkListStorage blobstore.BlobAccess[chunk.List]
	sourceChunkListFetcher chunk.ListFetcher
	sourceCdcParamsFetcher capabilities.CDCParametersFetcher
	sinkChunkStorage       blobstore.BlobAccess[*chunk.Chunk]
	sinkChunkListStorage   blobstore.BlobAccess[chunk.List]
	sinkCdcParamsFetcher   capabilities.CDCParametersFetcher
	instanceName           digest.InstanceName
}

// NewReplicator creates a Replicator that copies blobs from a source
// CAS, consisting of a Chunk Storage (CS) and Chunk List Storage (CLS),
// to a sink CAS.
func NewReplicator(
	zstdPool zstd.Pool,
	sourceChunkBytesReader reader.Reader[[]byte],
	sourceChunkListStorage blobstore.BlobAccess[chunk.List],
	sourceChunkListFetcher chunk.ListFetcher,
	sourceCdcParamsFetcher capabilities.CDCParametersFetcher,
	sinkChunkStorage blobstore.BlobAccess[*chunk.Chunk],
	sinkChunkListStorage blobstore.BlobAccess[chunk.List],
	sinkCdcParamsFetcher capabilities.CDCParametersFetcher,
	instanceName digest.InstanceName,
) Replicator {
	return &replicator{
		zstdPool:               zstdPool,
		sourceChunkBytesReader: sourceChunkBytesReader,
		sourceChunkListStorage: sourceChunkListStorage,
		sourceChunkListFetcher: sourceChunkListFetcher,
		sourceCdcParamsFetcher: sourceCdcParamsFetcher,
		sinkChunkStorage:       sinkChunkStorage,
		sinkChunkListStorage:   sinkChunkListStorage,
		sinkCdcParamsFetcher:   sinkCdcParamsFetcher,
		instanceName:           instanceName,
	}
}

func (r *replicator) Replicate(ctx context.Context, digests digest.Set) error {
	sourceParams, err := r.sourceCdcParamsFetcher.FetchCDCParameters(ctx, r.instanceName)
	if err != nil {
		return util.StatusWrap(err, "Could not determine source chunking parameters")
	}
	sinkParams, err := r.sinkCdcParamsFetcher.FetchCDCParameters(ctx, r.instanceName)
	if err != nil {
		return util.StatusWrap(err, "Could not determine sink chunking parameters")
	}

	missing, err := FindMissing(ctx, r.sinkChunkStorage, r.sinkChunkListStorage, sinkParams, digests)
	if err != nil {
		return util.StatusWrap(err, "Failed to determine which blobs to replicate")
	}

	if missing.Empty() {
		return nil
	}

	// As source and sink may differ in CDC parameters we fetch chunks
	// from source and put them as blobs towards the sink while
	// concatenating them with a final PutManifest call. This will cause
	// the backend to perform the cdc translation on the fly if
	// required.
	for _, blobDigest := range missing.Items() {
		if IsSingleChunk(sourceParams, blobDigest) {
			bytes, err := r.sourceChunkBytesReader.Read(ctx, blobDigest)
			if err != nil {
				return util.StatusWrapf(err, "Failed to fetch blob %s", blobDigest.String())
			}
			err = PutBytes(ctx, r.zstdPool, r.sinkChunkStorage, r.sinkChunkListStorage, sinkParams, blobDigest, bytes)
			if err != nil {
				return util.StatusWrapf(err, "Failed to replicate blob %s", blobDigest.String())
			}
			continue
		}
		chunkList, err := r.sourceChunkListFetcher.FetchChunkList(ctx, blobDigest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to fetch chunks for digest %s", blobDigest.String())
		}
		chunkDigestsBuilder := digest.NewSetBuilder(len(chunkList.Digests))
		for _, digest := range chunkList.Digests {
			chunkDigestsBuilder.Add(digest)
		}
		chunkDigests := chunkDigestsBuilder.Build()
		missingChunks, err := FindMissing(ctx, r.sinkChunkStorage, r.sinkChunkListStorage, sinkParams, chunkDigests)
		if err != nil {
			return util.StatusWrap(err, "Failed to determine which missing chunks were missing")
		}
		for _, chunkDigest := range missingChunks.Items() {
			bytes, err := r.sourceChunkBytesReader.Read(ctx, chunkDigest)
			if err != nil {
				return util.StatusWrapf(err, "Failed to fetch chunk %s of blob %s from source", chunkDigest.String(), blobDigest.String())
			}
			err = PutBytes(ctx, r.zstdPool, r.sinkChunkStorage, r.sinkChunkListStorage, sinkParams, chunkDigest, bytes)
			if err != nil {
				return util.StatusWrapf(err, "Failed to replicate chunk %s of blob %s to sink", chunkDigest.String(), blobDigest.String())
			}
		}
		if err := r.sinkChunkListStorage.Put(ctx, blobDigest, chunkList); err != nil {
			return util.StatusWrapf(err, "Failed to save chunk list for blob %s to sink", blobDigest.String())
		}
	}
	return nil
}
