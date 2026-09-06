package chunklist

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// ChunkFetcher retrieves the contents of an individual chunk.
type ChunkFetcher interface {
	FetchChunk(ctx context.Context, digest digest.Digest) ([]byte, error)
}

type functionChunkFetcher struct {
	fetcher func(ctx context.Context, digest digest.Digest) ([]byte, error)
}

// NewChunkFetcherFromFunction creates a chunk fetcher that wraps a
// function which fetches chunks.
func NewChunkFetcherFromFunction(fetcher func(ctx context.Context, digest digest.Digest) ([]byte, error)) ChunkFetcher {
	return functionChunkFetcher{fetcher: fetcher}
}

func (f functionChunkFetcher) FetchChunk(ctx context.Context, digest digest.Digest) ([]byte, error) {
	return f.fetcher(ctx, digest)
}
