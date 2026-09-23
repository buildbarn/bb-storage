package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type blobAccessChunkListFetcher struct {
	chunkListStorage BlobAccess[chunk.List]
}

// NewBlobAccessChunkListFetcher creates a ChunkListFetcher that reads
// chunk lists from the provided BlobAccess.
func NewBlobAccessChunkListFetcher(chunkListStorage BlobAccess[chunk.List]) chunk.ListFetcher {
	return &blobAccessChunkListFetcher{
		chunkListStorage: chunkListStorage,
	}
}

func (f *blobAccessChunkListFetcher) FetchChunkList(ctx context.Context, d digest.Digest) (chunk.List, error) {
	return f.chunkListStorage.Get(ctx, d)
}
