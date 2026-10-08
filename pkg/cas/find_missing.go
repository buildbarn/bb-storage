package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"golang.org/x/sync/errgroup"

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
		if d.GetSizeBytes() == 0 {
			// By definition in the Remote Execution API, a zero-sized
			// blob is always present.
		} else if IsSingleChunk(params, d) {
			smallDigests.Add(d)
		} else {
			largeDigests.Add(d)
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	var smallMissing, largeMissing digest.Set
	group.Go(func() error {
		var err error
		smallMissing, err = chunkStorage.FindMissing(groupCtx, smallDigests.Build())
		return err
	})
	group.Go(func() error {
		var err error
		largeMissing, err = chunkMappingStorage.FindMissing(groupCtx, largeDigests.Build())
		return err
	})
	if err := group.Wait(); err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion([]digest.Set{smallMissing, largeMissing}), nil
}
