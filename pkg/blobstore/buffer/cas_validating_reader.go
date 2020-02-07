package buffer

import (
	"bytes"
	"hash"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type casValidatingReader struct {
	io.ReadCloser
	digest         digest.Digest
	repairStrategy RepairStrategy

	err            error
	hasher         hash.Hash
	bytesRemaining int64
}

// newCASValidatingReader creates a decorator for io.ReadCloser that
// performs on-the-fly checksum validation of the contents as required
// by the Content Addressable Storage. It has been implemented in such a
// way that it does not allow access to the full stream's contents in
// case of size or checksum mismatches.
func newCASValidatingReader(r io.ReadCloser, digest digest.Digest, repairStrategy RepairStrategy) io.ReadCloser {
	return &casValidatingReader{
		ReadCloser:     r,
		digest:         digest,
		repairStrategy: repairStrategy,

		hasher:         digest.NewHasher(),
		bytesRemaining: digest.GetSizeBytes(),
	}
}

func (r *casValidatingReader) compareChecksum() error {
	expectedChecksum := r.digest.GetHashBytes()
	actualChecksum := r.hasher.Sum(nil)
	if bytes.Compare(expectedChecksum, actualChecksum) != 0 {
		return r.repairStrategy.repairCASHashMismatch(expectedChecksum, actualChecksum)
	}
	return nil
}

func (r *casValidatingReader) checkSize(n int) error {
	if int64(n) > r.bytesRemaining {
		sizeBytes := r.digest.GetSizeBytes()
		return r.repairStrategy.repairCASTooBig(sizeBytes, sizeBytes+int64(n)-r.bytesRemaining)
	}
	return nil
}

func (r *casValidatingReader) doRead(p []byte) (int, error) {
	n, readErr := r.ReadCloser.Read(p)
	if err := r.checkSize(n); err != nil {
		return 0, err
	}
	r.hasher.Write(p[:n])
	r.bytesRemaining -= int64(n)

	if readErr == io.EOF {
		// Compare the blob's size and checksum.
		if r.bytesRemaining != 0 {
			sizeBytes := r.digest.GetSizeBytes()
			return 0, r.repairStrategy.repairCASSizeMismatch(sizeBytes, sizeBytes-r.bytesRemaining)
		}
		if err := r.compareChecksum(); err != nil {
			return 0, err
		}
		return n, io.EOF
	} else if readErr != nil {
		return 0, readErr
	}

	if r.bytesRemaining == 0 {
		// No more data expected. We must observe an EOF now.
		var p [1]byte
		nFinal, err := io.ReadFull(r.ReadCloser, p[:])
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return 0, err
		}
		if err := r.checkSize(nFinal); err != nil {
			return 0, err
		}
		if err := r.compareChecksum(); err != nil {
			return 0, err
		}
		return n, io.EOF
	}
	return n, nil
}

func (r *casValidatingReader) Read(p []byte) (int, error) {
	// Return errors from previous iterations. This prevents
	// resumption of I/O after yielding a data integrity error once.
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.doRead(p)
	r.err = err
	return n, err
}
