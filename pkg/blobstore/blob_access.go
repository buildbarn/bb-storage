package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// capablitiesProvider is a copy of the capabilities.Provider interface
// which must be written inline for go mockgen to function in source
// mode.
type capabilitiesProvider interface {
	GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error)
}

var (
	_ capabilitiesProvider  = capabilities.Provider(nil)
	_ capabilities.Provider = capabilitiesProvider(nil)
)

// BlobAccess is an abstraction for a data store that can be used to
// hold an Action Cache (AC), Chunk Storage (CS), or any other data
// store that uses keys in the form of digests.
type BlobAccess[T any] interface {
	capabilitiesProvider

	Get(ctx context.Context, digest digest.Digest) (T, error)
	Put(ctx context.Context, digest digest.Digest, value T) error
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
// https://github.com/buildbarn/bb-adrs/blob/main/0003-cas-decomposition.md#future-work
const RecommendedFindMissingDigestsCount = 10000
