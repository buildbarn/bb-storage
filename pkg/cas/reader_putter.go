package cas

import (
	"bufio"
	"bytes"
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	cdc "github.com/buildbarn/go-cdc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReaderPutter abstracts putting streams of data into the Content
// Addressable Storage (CAS).
type ReaderPutter interface {
	// PutReader puts the contents of the reader into the CAS.
	PutReader(ctx context.Context, d digest.Digest, r io.Reader, params *remoteexecution.RepMaxCdcParams) error
	// PutReaderAt puts the contents of the reader into the CAS,
	// utilizing the random access afforded by the io.ReaderAt
	// interface.
	PutReaderAt(ctx context.Context, d digest.Digest, r io.ReaderAt, params *remoteexecution.RepMaxCdcParams) error
}

type readerPutter struct {
	chunkStorage        blobstore.BlobAccess[*chunk.Chunk]
	chunkMappingStorage blobstore.BlobAccess[chunk.Mapping]
	zstdPool            zstd.Pool
}

// NewReaderPutter creates a ReaderPutter that writes to the CAS.
func NewReaderPutter(chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], zstdPool zstd.Pool) ReaderPutter {
	return &readerPutter{
		chunkStorage:        chunkStorage,
		chunkMappingStorage: chunkMappingStorage,
		zstdPool:            zstdPool,
	}
}

func (rp *readerPutter) PutReader(ctx context.Context, d digest.Digest, r io.Reader, params *remoteexecution.RepMaxCdcParams) error {
	digestFunction := d.GetDigestFunction()
	cdcChunker := cdc.NewRepMaxContentDefinedChunker(
		&cdc.FastContentDefinedChunkerGearTable,
		int(params.MinChunkSizeBytes),
		int(params.HorizonSizeBytes),
	)
	chunkReader := cdcChunker.NewChunkReader(
		bufio.NewReaderSize(r, cdcChunker.GetMaximumPeekSizeBytes()),
	)
	wholeGen := digestFunction.NewGenerator(d.GetSizeBytes())

	chunkDigests := make([]digest.Digest, 0)
	var offset uint64
	for {
		chunkData, err := chunkReader.ReadNextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.StatusWrap(err, "Failed to chunk write stream")
		}

		// The chunker returns data that aliases an internal buffer
		// which gets reused between calls. Copy it, so that the
		// resulting chunk may be retained for as long as desired.
		data := bytes.Clone(chunkData)

		digestGenerator := digestFunction.NewGenerator(int64(len(data)))
		if _, err := digestGenerator.Write(data); err != nil {
			return status.Error(codes.Internal, "Could not compute digest of chunk")
		}
		chunkDigest := digestGenerator.Sum()

		if _, err := wholeGen.Write(data); err != nil {
			return status.Error(codes.Internal, "Could not compute digest of blob")
		}

		// TODO: If we allow ourselves to gather a couple of chunks we
		// can do fewer find missing calls and reduce pipelining delay
		// as we can chunk as fast as possible. This does require some
		// form of semaphore to prevent memory use from growing
		// uncontrollably.
		missing, err := rp.chunkStorage.FindMissing(ctx, chunkDigest.ToSingletonSet())
		if err != nil {
			return err
		}
		if !missing.Empty() {
			// The calculated chunk was not present in Chunk Storage.
			if err := rp.chunkStorage.Put(ctx, chunkDigest, chunk.NewChunk(rp.zstdPool, data)); err != nil {
				return util.StatusWrap(err, "Failed to save chunk")
			}
		}

		chunkDigests = append(chunkDigests, chunkDigest)
		offset += uint64(chunkDigest.GetSizeBytes())
		if offset > uint64(d.GetSizeBytes()) {
			return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, digest is supposed to be %d bytes but have already received %d bytes", d.GetSizeBytes(), offset)
		}
	}

	// Verify the whole blob against the advertised digest.
	if actual := wholeGen.Sum(); actual != d {
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, advertised %s, actual %s", d, actual)
	}

	// A single chunk is the trivial case: it already lives in the
	// chunk storage and needs no chunk mapping.
	if len(chunkDigests) <= 1 {
		return nil
	}

	chunkMapping, err := chunk.NewMappingFromDigests(chunkDigests, uint64(d.GetSizeBytes()), true)
	if err != nil {
		return err
	}
	if err := rp.chunkMappingStorage.Put(ctx, d, chunkMapping); err != nil {
		return util.StatusWrap(err, "Could not save chunk mapping for blob")
	}
	return nil
}

func (rp *readerPutter) PutReaderAt(ctx context.Context, d digest.Digest, r io.ReaderAt, params *remoteexecution.RepMaxCdcParams) error {
	// TODO: As readerAt allows random access we can calculate all
	// chunks ahead of time with the parallelism afforded by repmaxcdc.
	// We can then check FMB for all the chunks and only upload any
	// missing ones.
	return rp.PutReader(ctx, d, io.NewSectionReader(r, 0, d.GetSizeBytes()), params)
}
