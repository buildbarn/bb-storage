package cdc

import (
	"bufio"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
	cdc "github.com/buildbarn/go-cdc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readerChunker struct {
	cdcChunker     cdc.ContentDefinedChunker
	reader         io.Reader
	digestFunction digest.Function
}

func (c *readerChunker) NextChunk() (Chunk, error) {
	chunk, err := c.cdcChunker.ReadNextChunk()
	if err != nil {
		return Chunk{}, err
	}

	digestGenerator := c.digestFunction.NewGenerator(int64(len(chunk)))
	if _, err := digestGenerator.Write(chunk); err != nil {
		return Chunk{}, status.Error(codes.Internal, "Could not compute digest of chunk")
	}
	chunkDigest := digestGenerator.Sum()

	return Chunk{
		Data:   chunk,
		Digest: chunkDigest,
	}, nil
}

// NewReaderChunker creates a chunker that reads from an io.Reader
func NewReaderChunker(digestFunction digest.Function, reader io.Reader, minChunkSizeBytes, horizonSizeBytes int64) Chunker {
	// The internal RepMaxContentDefinedChunker may peek up to this many
	// bytes. We therefore make sure that the underlying buffer is big
	// enough to prevent bufio.ErrBufferFull errors.
	bufferSizeBytes := 2*minChunkSizeBytes + horizonSizeBytes
	return &readerChunker{
		cdc.NewRepMaxContentDefinedChunker(
			bufio.NewReaderSize(reader, int(bufferSizeBytes)),
			&cdc.FastContentDefinedChunkerGearTable,
			int(minChunkSizeBytes),
			int(horizonSizeBytes),
		),
		reader,
		digestFunction,
	}
}
