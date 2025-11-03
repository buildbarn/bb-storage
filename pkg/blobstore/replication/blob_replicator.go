package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BlobReplicator provides the strategy that is used by
// MirroredBlobAccess to replicate objects between storage backends.
// This strategy is called into when MirroredBlobAccess detects that a
// certain object is only present in one of the two backends.
type BlobReplicator interface {
	// Replicate a single object between backends, while at the same
	// time giving a handle back to it.
	ReplicateSingle(ctx context.Context, digest digest.Digest) buffer.Buffer
	// Replicate a single composite object between backends, while
	// at the same time giving a handle back to one of its children.
	ReplicateComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer
	// Replicate a set of objects between backends.
	ReplicateMultiple(ctx context.Context, digests digest.Set) error
}

// notFoundToInternalErrorHandler is a helper type that implementations
// of BlobReplicator with simple implementations of ReplicateSingle()
// can use to ensure that absence of an object in the sink does not
// cause a request to fail with a NOT_FOUND error. Only NOT_FOUND errors
// on the source may be propagated.
type notFoundToInternalErrorHandler struct{}

func (notFoundToInternalErrorHandler) OnError(err error) (buffer.Buffer, error) {
	if status.Code(err) == codes.NotFound {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Blob absent from sink after replication")
	}
	return nil, err
}

func (notFoundToInternalErrorHandler) Done() {}
