package cas

import (
	"bytes"
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamReader reads a blob from the Content Addressable Storage (CAS)
// in a streaming fashion. The returned reader validates the contents
// of the blob against its digest.
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
	if IsSingleChunk(params, d) {
		// The digest of a blob stored as a single chunk is the
		// key under which it is stored, so its contents cannot
		// mismatch.
		chunkBytes, err := r.chunkBytesReader.Read(ctx, d)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(chunkBytes), nil
	}
	mapping, err := r.chunkMappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch chunk mapping")
	}
	return &validatingMappingReader{
		ctx:              ctx,
		chunkBytesReader: r.chunkBytesReader,
		chunkDigests:     mapping.Digests,
		blobDigest:       d,
		digestGenerator:  d.GetDigestFunction().NewGenerator(d.GetSizeBytes()),
	}, nil
}

// validatingMappingReader is an io.Reader that stitches together the
// contents of a blob based on an ordered mapping of chunk digests. The
// chunks are required to concatenate to the blob digest, which is
// verified upon the final chunk of the mapping being fetched.
type validatingMappingReader struct {
	ctx              context.Context
	chunkBytesReader reader.Reader[[]byte]
	chunkDigests     []digest.Digest
	blobDigest       digest.Digest
	digestGenerator  *digest.Generator

	currentChunkIndex  int
	currentChunkData   []byte
	currentChunkOffset int
	err                error
}

func (r *validatingMappingReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	// Fetch the next chunk if the current one is exhausted.
	if r.currentChunkOffset >= len(r.currentChunkData) {
		if r.currentChunkIndex >= len(r.chunkDigests) {
			return 0, io.EOF
		}
		chunkDigest := r.chunkDigests[r.currentChunkIndex]
		chunkData, err := r.chunkBytesReader.Read(r.ctx, chunkDigest)
		if err != nil {
			r.err = util.StatusWrapf(err, "Failed to fetch chunk at index %d", r.currentChunkIndex)
			return 0, r.err
		}
		if _, err := r.digestGenerator.Write(chunkData); err != nil {
			r.err = status.Error(codes.Internal, "Failed to compute digest of blob")
			return 0, r.err
		}
		r.currentChunkData = chunkData
		r.currentChunkOffset = 0
		r.currentChunkIndex++
		if r.currentChunkIndex == len(r.chunkDigests) {
			// The final chunk of the mapping was fetched. At this point
			// we can verify the digest.
			if actual := r.digestGenerator.Sum(); actual != r.blobDigest {
				r.err = status.Errorf(
					codes.Internal,
					"Blob digest mismatch, advertised %s, actual %s",
					r.blobDigest,
					actual,
				)
				// The reader is useless now.
				return 0, r.err
			}
		}
	}

	// Copy as much data as available from the current chunk into p.
	n := copy(p, r.currentChunkData[r.currentChunkOffset:])
	r.currentChunkOffset += n

	if r.currentChunkOffset >= len(r.currentChunkData) {
		r.currentChunkData = nil
	}

	return n, nil
}
