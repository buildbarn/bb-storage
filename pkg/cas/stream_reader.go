package cas

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// StreamReader reads a blob from the Content Addressable Storage (CAS)
// in a streaming fashion.
type StreamReader interface {
	// Read a blob from the CAS with the specific digest.
	ReadStream(ctx context.Context, d digest.Digest) (io.Reader, error)
}

type storageBackedStreamReader struct {
	chunkBytesReader     reader.Reader[[]byte]
	chunkMappingFetcher  chunk.MappingFetcher
	cdcParametersFetcher capabilities.CDCParametersFetcher
}

// NewStorageBackedStreamReader creates a stream reader that reads from
// the provided Chunk Storage (CS) and Chunk Mapping Storage (CMS).
func NewStorageBackedStreamReader(chunkBytesReader reader.Reader[[]byte], chunkMappingFetcher chunk.MappingFetcher, cdcParametersFetcher capabilities.CDCParametersFetcher) StreamReader {
	return &storageBackedStreamReader{
		chunkBytesReader:     chunkBytesReader,
		chunkMappingFetcher:  chunkMappingFetcher,
		cdcParametersFetcher: cdcParametersFetcher,
	}
}

func (r *storageBackedStreamReader) ReadStream(ctx context.Context, d digest.Digest) (io.Reader, error) {
	params, err := r.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch CDC parameters")
	}
	return GetReader(ctx, r.chunkBytesReader, r.chunkMappingFetcher, params, d, 0)
}
