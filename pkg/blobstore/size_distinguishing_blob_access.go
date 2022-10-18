package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
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
func NewSizeDistinguishingBlobAccess(smallBlobAccess, largeBlobAccess BlobAccess, cutoffSizeBytes int64) BlobAccess {
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

func (ba *sizeDistinguishingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	if parentDigest.GetSizeBytes() <= ba.cutoffSizeBytes {
		return ba.smallBlobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer)
	}
	return ba.largeBlobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer)
}

func (ba *sizeDistinguishingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	// Use the size that's in the digest; not the size provided. We
	// can't re-obtain that in the other operations.
	if digest.GetSizeBytes() <= ba.cutoffSizeBytes {
		return ba.smallBlobAccess.Put(ctx, digest, b)
	}
	return ba.largeBlobAccess.Put(ctx, digest, b)
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
	group, groupCtx := errgroup.WithContext(ctx)
	var smallResults, largeResults digest.Set
	group.Go(func() error {
		var err error
		smallResults, err = ba.smallBlobAccess.FindMissing(groupCtx, smallDigests.Build())
		if err != nil {
			return util.StatusWrap(err, "Small backend")
		}
		return nil
	})
	group.Go(func() error {
		var err error
		largeResults, err = ba.largeBlobAccess.FindMissing(groupCtx, largeDigests.Build())
		if err != nil {
			return util.StatusWrap(err, "Large backend")
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		return digest.EmptySet, nil
	}

	// Recombine results.
	return digest.GetUnion([]digest.Set{smallResults, largeResults}), nil
}

func (ba *sizeDistinguishingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Assume that the capabilities announced by the small backend
	// are also valid for the large backend.
	return ba.smallBlobAccess.GetCapabilities(ctx, instanceName)
}
