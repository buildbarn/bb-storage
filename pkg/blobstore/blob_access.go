package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// BlobAccess is an abstraction for a data store that can be used to
// hold both a Bazel Action Cache (AC) and Content Addressable Storage
// (CAS).
type BlobAccess interface {
	Get(ctx context.Context, digest *util.Digest) buffer.Buffer
	Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error
	FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error)
}
