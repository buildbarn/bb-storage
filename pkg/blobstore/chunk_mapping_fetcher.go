package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type blobAccessChunkMappingFetcher struct {
	chunkMappingStorage BlobAccess[chunk.Mapping]
}

// NewBlobAccessMappingFetcher creates a ChunkMappingFetcher that reads
// chunk mappings from the provided BlobAccess.
func NewBlobAccessMappingFetcher(chunkMappingStorage BlobAccess[chunk.Mapping]) chunk.MappingFetcher {
	return &blobAccessChunkMappingFetcher{
		chunkMappingStorage: chunkMappingStorage,
	}
}

func (f *blobAccessChunkMappingFetcher) FetchChunkMapping(ctx context.Context, d digest.Digest) (chunk.Mapping, error) {
	return f.chunkMappingStorage.Get(ctx, d)
}
