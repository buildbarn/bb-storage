package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type sizeDistinguishingBlobAccess struct {
	smallBlobAccess BlobAccess
	largeBlobAccess BlobAccess
	cutoffSizeBytes int64
}

// NewSizeDistinguishingBlobAccess creates a BlobAccess that splits up
// requests between two backends based on the size of the object
// specified in the digest. Backends tend to have different performance
// characteristics based on blob size. This adapter may be used to
// optimize performance based on that.
func NewSizeDistinguishingBlobAccess(smallBlobAccess BlobAccess, largeBlobAccess BlobAccess, cutoffSizeBytes int64) BlobAccess {
	return &sizeDistinguishingBlobAccess{
		smallBlobAccess: smallBlobAccess,
		largeBlobAccess: largeBlobAccess,
		cutoffSizeBytes: cutoffSizeBytes,
	}
}

func (ba *sizeDistinguishingBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	if digest.GetSizeBytes() <= ba.cutoffSizeBytes {
		return ba.smallBlobAccess.Get(ctx, digest)
	}
	return ba.largeBlobAccess.Get(ctx, digest)
}

func (ba *sizeDistinguishingBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	// Use the size that's in the digest; not the size provided. We
	// can't re-obtain that in the other operations.
	if digest.GetSizeBytes() <= ba.cutoffSizeBytes {
		return ba.smallBlobAccess.Put(ctx, digest, b)
	}
	return ba.largeBlobAccess.Put(ctx, digest, b)
}

type findMissingResults struct {
	missing []*util.Digest
	err     error
}

func callFindMissing(ctx context.Context, blobAccess BlobAccess, digests []*util.Digest) findMissingResults {
	missing, err := blobAccess.FindMissing(ctx, digests)
	return findMissingResults{missing: missing, err: err}
}

func (ba *sizeDistinguishingBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	// Split up digests by size.
	var smallDigests []*util.Digest
	var largeDigests []*util.Digest
	for _, digest := range digests {
		if digest.GetSizeBytes() <= ba.cutoffSizeBytes {
			smallDigests = append(smallDigests, digest)
		} else {
			largeDigests = append(largeDigests, digest)
		}
	}

	// Forward FindMissing() to both implementations.
	smallResultsChan := make(chan findMissingResults, 1)
	go func() {
		smallResultsChan <- callFindMissing(ctx, ba.smallBlobAccess, smallDigests)
	}()
	largeResults := callFindMissing(ctx, ba.largeBlobAccess, largeDigests)
	smallResults := <-smallResultsChan

	// Recombine results.
	if smallResults.err != nil {
		return nil, smallResults.err
	}
	if largeResults.err != nil {
		return nil, largeResults.err
	}
	return append(smallResults.missing, largeResults.missing...), nil
}
