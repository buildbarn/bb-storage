package buffer

import (
	"bytes"
	"hash"
	"io"

	"github.com/buildbarn/bb-storage/pkg/util"
)

type casValidatingChunkReader struct {
	ChunkReader
	digest         *util.Digest
	repairStrategy RepairStrategy

	err            error
	hasher         hash.Hash
	bytesRemaining int64
}

// newCASValidatingChunkReader creates a decorator for ChunkReader that
// performs on-the-fly checksum validation of the contents as required
// by the Content Addressable Storage. It has been implemented in such a
// way that it does not allow access to the full stream's contents in
// case of size or checksum mismatches.
func newCASValidatingChunkReader(r ChunkReader, digest *util.Digest, repairStrategy RepairStrategy) ChunkReader {
	return &casValidatingChunkReader{
		ChunkReader:    r,
		digest:         digest,
		repairStrategy: repairStrategy,

		hasher:         digest.NewHasher(),
		bytesRemaining: digest.GetSizeBytes(),
	}
}

func (r *casValidatingChunkReader) checkSize(chunkLength int) error {
	if int64(chunkLength) > r.bytesRemaining {
		sizeBytes := r.digest.GetSizeBytes()
		return r.repairStrategy.repairCASTooBig(sizeBytes, sizeBytes+int64(chunkLength)-r.bytesRemaining)
	}
	return nil
}

func (r *casValidatingChunkReader) maybeFinalize() error {
	if r.bytesRemaining > 0 {
		return nil
	}

	// Check that there aren't any non-empty trailing chunks.
	for {
		chunk, err := r.ChunkReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if err := r.checkSize(len(chunk)); err != nil {
			return err
		}
	}

	// Compare the blob's checksum.
	expectedChecksum := r.digest.GetHashBytes()
	actualChecksum := r.hasher.Sum(nil)
	if bytes.Compare(expectedChecksum, actualChecksum) != 0 {
		return r.repairStrategy.repairCASHashMismatch(expectedChecksum, actualChecksum)
	}
	return io.EOF
}

func (r *casValidatingChunkReader) doRead() ([]byte, error) {
	if err := r.maybeFinalize(); err != nil {
		return nil, err
	}

	chunk, err := r.ChunkReader.Read()
	if err == io.EOF {
		// Premature end-of-file.
		sizeBytes := r.digest.GetSizeBytes()
		return nil, r.repairStrategy.repairCASSizeMismatch(sizeBytes, sizeBytes-r.bytesRemaining)
	} else if err != nil {
		return nil, err
	}

	if err := r.checkSize(len(chunk)); err != nil {
		return nil, err
	}
	r.hasher.Write(chunk)
	r.bytesRemaining -= int64(len(chunk))
	return chunk, nil
}

func (r *casValidatingChunkReader) Read() ([]byte, error) {
	// Return errors from previous iterations.
	if r.err != nil {
		return nil, r.err
	}

	// Read the next chunk of data.
	var chunk []byte
	chunk, r.err = r.doRead()
	if r.err != nil {
		return nil, r.err
	}

	// If we're about to return the last actual chunk of data,
	// already finalize the reader. This prevents us from returning
	// all of the data in case of data inconsistencies.
	r.err = r.maybeFinalize()
	if r.err != nil && r.err != io.EOF {
		return nil, r.err
	}
	return chunk, nil
}
