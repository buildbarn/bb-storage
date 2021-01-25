package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BlobAccess is an abstraction for a data store that can be used to
// hold both a Bazel Action Cache (AC) and Content Addressable Storage
// (CAS).
type BlobAccess interface {
	Get(ctx context.Context, digest digest.Digest) buffer.Buffer
	Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error
	FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error)
}

// RecommendedFindMissingDigestsCount corresponds to the maximum number
// of digests that is safe to provide to BlobAccess.FindMissing()
// without running into size limits of underlying protocols.
//
// Assuming that a typical REv2 Digest message is ~70 bytes in size,
// this will cause us to generate gRPC calls with messages of up to ~700
// KB in size. This seems like a safe limit, because most gRPC
// implementations limit the maximum message size to a small number of
// megabytes (4 MB for Java, 16 MB for Go).
//
// TODO: Would it make sense to replace BlobAccess.FindMissing() with a
// streaming API that abstracts away the maximum digests count? That way
// we can ensure proper batching of digests, even when sharding is used,
// ExistenceCachingBlobAccess has a high hit rate, etc..
//
// It would be nice if such an API also supported decomposition of large
// objects natively. See the "Future work" section in ADR#3 for details:
// https://github.com/buildbarn/bb-adrs/blob/master/0003-cas-decomposition.md#future-work
const RecommendedFindMissingDigestsCount = 10000
