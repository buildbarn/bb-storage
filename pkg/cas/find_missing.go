package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// FindMissing returns the digests from the given set that are not
// present in the CAS. Digests smaller than twice the minimum chunk size
// of the CDC parameters are looked up in the Chunk Storage (CS), while
// larger digests are looked up in the Chunk List Storage (CLS). All
// digests must belong to the instance name that the CDC parameters were
// fetched for.
func FindMissing(ctx context.Context, chunkStorage blobstore.BlobAccess[*buffer.Chunk], chunkListStorage blobstore.BlobAccess[chunklist.ChunkList], params *remoteexecution.RepMaxCdcParams, digests digest.Set) (digest.Set, error) {
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
	largeMissing, err := chunkListStorage.FindMissing(ctx, largeDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion([]digest.Set{smallMissing, largeMissing}), nil
}
