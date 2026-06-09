package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type blobAccessChunkListFetcher struct {
	chunkListStorage BlobAccess[chunklist.ChunkList]
}

// NewBlobAccessChunkListFetcher creates a ChunkListFetcher that reads
// chunk lists from the provided BlobAccess.
func NewBlobAccessChunkListFetcher(chunkListStorage BlobAccess[chunklist.ChunkList]) chunklist.Fetcher {
	return &blobAccessChunkListFetcher{
		chunkListStorage: chunkListStorage,
	}
}

func (f *blobAccessChunkListFetcher) FetchChunkList(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	return f.chunkListStorage.Get(ctx, d)
}
