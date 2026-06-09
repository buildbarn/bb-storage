package cas

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// StreamReader reads a blob from the Content Addressable Storage (CAS)
// in a streaming fashion.
type StreamReader interface {
	// Read a blob from the CAS with the specific digest.
	ReadStream(ctx context.Context, d digest.Digest) (io.ReadCloser, error)
}

type storageBackedStreamReader struct {
	chunkBytesReader     reader.Reader[[]byte]
	chunkListFetcher     chunklist.Fetcher
	cdcParametersFetcher cdc.ParametersFetcher
}

// NewStorageBackedStreamReader creates a stream reader that reads from
// the provided Chunk Storage (CS) and Chunk List Storage (CLS).
func NewStorageBackedStreamReader(chunkBytesReader reader.Reader[[]byte], chunkListFetcher chunklist.Fetcher, cdcParametersFetcher cdc.ParametersFetcher) StreamReader {
	return &storageBackedStreamReader{
		chunkBytesReader:     chunkBytesReader,
		chunkListFetcher:     chunkListFetcher,
		cdcParametersFetcher: cdcParametersFetcher,
	}
}

func (r *storageBackedStreamReader) ReadStream(ctx context.Context, d digest.Digest) (io.ReadCloser, error) {
	params, err := r.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch CDC parameters")
	}
	return GetReadCloserAt(ctx, r.chunkBytesReader, r.chunkListFetcher, params, d, 0)
}
