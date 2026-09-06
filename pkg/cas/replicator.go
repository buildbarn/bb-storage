package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// Replicator replicates blobs from a source Content Addressable Storage
// (CAS) to a sink CAS.
type Replicator interface {
	// Replicate blobs from the set of digests.
	Replicate(ctx context.Context, digests digest.Set) error
}

type replicator struct {
	source       ContentAddressableStorage
	sink         ContentAddressableStorage
	instanceName digest.InstanceName
}

// NewReplicator creates a Replicator from a source and sink CAS.
func NewReplicator(source, sink ContentAddressableStorage, instanceName digest.InstanceName) Replicator {
	return &replicator{
		source:       source,
		sink:         sink,
		instanceName: instanceName,
	}
}

func (r *replicator) Replicate(ctx context.Context, digests digest.Set) error {
	missing, err := r.sink.FindMissing(ctx, digests)
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
	sourceParams, err := r.source.FetchCDCParameters(ctx, r.instanceName)
	if err != nil {
		return util.StatusWrap(err, "Could not determine source chunking parameters")
	}

	for _, blobDigest := range missing.Items() {
		if IsSingleChunk(sourceParams, blobDigest) {
			bytes, err := r.source.FetchChunk(ctx, blobDigest)
			if err != nil {
				return util.StatusWrapf(err, "Failed to fetch blob %s", blobDigest.String())
			}
			err = PutBytes(ctx, r.sink, blobDigest, bytes)
			if err != nil {
				return util.StatusWrapf(err, "Failed to replicate blob %s", blobDigest.String())
			}
		}
		chunkList, err := r.source.GetManifest(ctx, blobDigest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to fetch chunks for digest %s", blobDigest.String())
		}
		chunkDigestsBuilder := digest.NewSetBuilder(len(chunkList))
		for _, chunk := range chunkList {
			chunkDigestsBuilder.Add(chunk.Digest)
		}
		chunkDigests := chunkDigestsBuilder.Build()
		missingChunks, err := r.sink.FindMissing(ctx, chunkDigests)
		if err != nil {
			return util.StatusWrap(err, "Failed to determine which missing chunks were missing")
		}
		for _, chunkDigest := range missingChunks.Items() {
			bytes, err := r.source.FetchChunk(ctx, chunkDigest)
			if err != nil {
				return util.StatusWrapf(err, "Failed to fetch chunk %s of blob %s from source", chunkDigest.String(), blobDigest.String())
			}
			err = PutBytes(ctx, r.sink, chunkDigest, bytes)
			if err != nil {
				return util.StatusWrapf(err, "Failed to replicate chunk %s of blob %s to sink", chunkDigest.String(), blobDigest.String())
			}
		}
		err = r.sink.PutManifest(ctx, blobDigest, chunkList)
		if err != nil {
			return util.StatusWrapf(err, "Failed to save chunk list for blob %s to sink", blobDigest.String())
		}
	}
	return nil
}
