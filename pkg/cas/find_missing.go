package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// FindMissing returns the digests from the given set that are not
// present in the CAS. Digests smaller than twice the minimum chunk size
// of the CDC parameters are looked up in the Chunk Storage (CS), while
// larger digests are looked up in the Chunk Mapping Storage (CMS). All
// digests must belong to the instance name that the CDC parameters were
// fetched for.
func FindMissing(ctx context.Context, chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], params *remoteexecution.RepMaxCdcParams, digests digest.Set) (digest.Set, error) {
	smallDigests := digest.NewSetBuilder(digests.Length())
	largeDigests := digest.NewSetBuilder(digests.Length())
	for _, d := range digests.Items() {
		if IsSingleChunk(params, d) {
			smallDigests.Add(d)
		} else {
			largeDigests.Add(d)
		}
	}
	smallMissing, err := chunkStorage.FindMissing(ctx, smallDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	largeMissing, err := chunkMappingStorage.FindMissing(ctx, largeDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion([]digest.Set{smallMissing, largeMissing}), nil
}
