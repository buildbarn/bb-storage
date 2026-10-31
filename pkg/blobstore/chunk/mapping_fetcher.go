package chunk

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// MappingFetcher retrieves a ChunkMapping for a digest.
type MappingFetcher interface {
	FetchChunkMapping(ctx context.Context, digest digest.Digest) (Mapping, error)
}
