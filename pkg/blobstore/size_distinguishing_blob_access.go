package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
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

func (ba *sizeDistinguishingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	if digest.GetSizeBytes() <= ba.cutoffSizeBytes {
		return ba.smallBlobAccess.Get(ctx, digest)
	}
	return ba.largeBlobAccess.Get(ctx, digest)
}

func (ba *sizeDistinguishingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	// Use the size that's in the digest; not the size provided. We
	// can't re-obtain that in the other operations.
	if digest.GetSizeBytes() <= ba.cutoffSizeBytes {
		return ba.smallBlobAccess.Put(ctx, digest, b)
	}
	return ba.largeBlobAccess.Put(ctx, digest, b)
}

type findMissingResults struct {
	missing digest.Set
	err     error
}

func callFindMissing(ctx context.Context, blobAccess BlobAccess, digests digest.Set) findMissingResults {
	missing, err := blobAccess.FindMissing(ctx, digests)
	return findMissingResults{missing: missing, err: err}
}

func (ba *sizeDistinguishingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Split up digests by size.
	smallDigests := digest.NewSetBuilder()
	largeDigests := digest.NewSetBuilder()
	for _, digest := range digests.Items() {
		if digest.GetSizeBytes() <= ba.cutoffSizeBytes {
			smallDigests.Add(digest)
		} else {
			largeDigests.Add(digest)
		}
	}

	// Forward FindMissing() to both implementations.
	smallResultsChan := make(chan findMissingResults, 1)
	go func() {
		smallResultsChan <- callFindMissing(ctx, ba.smallBlobAccess, smallDigests.Build())
	}()
	largeResults := callFindMissing(ctx, ba.largeBlobAccess, largeDigests.Build())
	smallResults := <-smallResultsChan

	// Recombine results.
	if smallResults.err != nil {
		return digest.EmptySet, smallResults.err
	}
	if largeResults.err != nil {
		return digest.EmptySet, largeResults.err
	}
	return digest.GetUnion([]digest.Set{smallResults.missing, largeResults.missing}), nil
}
