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
	cdc "github.com/buildbarn/go-cdc"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecommendedMaximumBufferedChunks is a recommended size for the
// semaphore given to NewReaderPutter(). It is used if the
// maximum_buffered_chunks option of the CAS configuration is not set.
const RecommendedMaximumBufferedChunks = 64

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
	chunkSemaphore      *semaphore.Weighted
}

// NewReaderPutter creates a ReaderPutter that writes to the CAS.
func NewReaderPutter(chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], chunkSemaphore *semaphore.Weighted) ReaderPutter {
	return &readerPutter{
		chunkStorage:        chunkStorage,
		chunkMappingStorage: chunkMappingStorage,
		chunkSemaphore:      chunkSemaphore,
	}
}

// storeChunk stores a chunk in the Chunk Storage, if it is missing.
func (rp *readerPutter) storeChunk(ctx context.Context, d digest.Digest, c *chunk.Chunk) error {
	missing, err := rp.chunkStorage.FindMissing(ctx, d.ToSingletonSet())
	if err != nil {
		return err
	}
	if !missing.Empty() {
		if err := rp.chunkStorage.Put(ctx, d, c); err != nil {
			return util.StatusWrap(err, "Failed to save chunk")
		}
	}
	return nil
}

func (rp *readerPutter) PutReader(ctx context.Context, d digest.Digest, r io.Reader, params *remoteexecution.RepMaxCdcParams) error {
	digestFunction := d.GetDigestFunction()

	if IsSingleChunk(params, d) {
		// Blob will fit in a single chunk, so we read it directly
		// making sure the stream does not add up to more bytes than
		// advertised.
		data, err := io.ReadAll(io.LimitReader(r, d.GetSizeBytes()+1))
		if err != nil {
			return util.StatusWrap(err, "Failed to read write stream")
		}
		if int64(len(data)) > d.GetSizeBytes() {
			return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, digest is supposed to be %d bytes but have already received %d bytes", d.GetSizeBytes(), len(data))
		}
		c := chunk.NewChunk(data)
		if actual := c.ComputeDigest(digestFunction); actual != d {
			return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, advertised %s, actual %s", d, actual)
		}
		if d.GetSizeBytes() > 0 {
			return rp.storeChunk(ctx, d, c)
		}
		return nil
	}

	cdcChunker := cdc.NewRepMaxContentDefinedChunker(
		&cdc.FastContentDefinedChunkerGearTable,
		int(params.MinChunkSizeBytes),
		int(params.HorizonSizeBytes),
	)
	chunkReader := cdcChunker.NewChunkReader(
		bufio.NewReaderSize(r, cdcChunker.GetMaximumPeekSizeBytes()),
	)
	wholeGen := digestFunction.NewGenerator(d.GetSizeBytes())

	group, groupCtx := errgroup.WithContext(ctx)
	chunkDigests := make([]digest.Digest, 0)
	readErr := func() error {
		var offset uint64
		for {
			chunkData, err := chunkReader.ReadNextChunk()
			if err == io.EOF {
				break
			}
			if err != nil {
				return util.StatusWrap(err, "Failed to chunk write stream")
			}

			if err := util.AcquireSemaphore(groupCtx, rp.chunkSemaphore, 1); err != nil {
				return err
			}

			// The chunker returns data that aliases an internal
			// buffer which gets reused between calls. Copy it, so
			// that the chunk may be retained until it is stored.
			c := chunk.NewChunk(bytes.Clone(chunkData))
			chunkDigest := c.ComputeDigest(digestFunction)

			if _, err := wholeGen.Write(c.GetBytes()); err != nil {
				rp.chunkSemaphore.Release(1)
				return status.Error(codes.Internal, "Could not compute digest of blob")
			}
			chunkDigests = append(chunkDigests, chunkDigest)
			offset += uint64(chunkDigest.GetSizeBytes())
			if offset > uint64(d.GetSizeBytes()) {
				rp.chunkSemaphore.Release(1)
				return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, digest is supposed to be %d bytes but have already received %d bytes", d.GetSizeBytes(), offset)
			}

			group.Go(func() error {
				defer rp.chunkSemaphore.Release(1)
				return rp.storeChunk(groupCtx, chunkDigest, c)
			})
		}

		// Verify the whole blob against the advertised digest.
		if actual := wholeGen.Sum(); actual != d {
			return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, advertised %s, actual %s", d, actual)
		}
		return nil
	}()

	// A failed store cancels groupCtx, which can cause readErr. Thus,
	// the error of the store has priority.
	if err := group.Wait(); err != nil {
		return err
	}
	if readErr != nil {
		return readErr
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
