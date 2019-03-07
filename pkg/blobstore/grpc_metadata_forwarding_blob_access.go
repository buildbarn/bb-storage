package blobstore

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
)

type grpcMetadataForwardingBlobAccess struct {
	remoteBlobAccess BlobAccess
}

// NewGRPCMetadataForwardingBlobAccess creates a BlobAccess that converts
// incoming client request metadata to outcoming metadata before delegating
// the actual request handling to an underlying GRPC BlobAccess.
func NewGRPCMetadataForwardingBlobAccess(remoteBlobAccess BlobAccess) BlobAccess {
	return &grpcMetadataForwardingBlobAccess{
		remoteBlobAccess: remoteBlobAccess,
	}
}

func (ba *grpcMetadataForwardingBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	ctx = metautils.ExtractIncoming(ctx).ToOutgoing(ctx)
	return ba.remoteBlobAccess.Get(ctx, digest)
}

func (ba *grpcMetadataForwardingBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	ctx = metautils.ExtractIncoming(ctx).ToOutgoing(ctx)
	return ba.remoteBlobAccess.Put(ctx, digest, sizeBytes, r)
}

func (ba *grpcMetadataForwardingBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	ctx = metautils.ExtractIncoming(ctx).ToOutgoing(ctx)
	return ba.remoteBlobAccess.Delete(ctx, digest)
}

func (ba *grpcMetadataForwardingBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ctx = metautils.ExtractIncoming(ctx).ToOutgoing(ctx)
	return ba.remoteBlobAccess.FindMissing(ctx, digests)
}
