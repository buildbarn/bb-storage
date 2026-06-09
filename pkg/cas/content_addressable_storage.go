package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// ContentAddressableStorage is an interface which describes
// interactions with a Content Addressable Storage (CAS).
type ContentAddressableStorage interface {
	// GetDigestKeyFormat returns the combined digest key format of the
	// CS and CLS.
	GetDigestKeyFormat() digest.KeyFormat
	// FetchCDCParameters fetches the CDC parameters of the CAS.
	FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (cdc.Parameters, error)
	// FindMissing returns the set of digests that are not present in
	// the CAS.
	FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error)
	// FetchChunk fetches the contents of a specific chunk.
	FetchChunk(ctx context.Context, d digest.Digest) ([]byte, error)
	// PutChunk stores the contents of a specific chunk.
	PutChunk(ctx context.Context, d digest.Digest, data []byte) error
	// GetManifest returns the chunk list which constitutes a specific
	// blob.
	GetManifest(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error)
	// PutManifest stores the chunk list which constitutes a specific
	// blob.
	PutManifest(ctx context.Context, d digest.Digest, manifest chunklist.ChunkList) error
}

// ContentAddressableStorage implements the chunklist.ChunkFetcher
// interface
var _ chunklist.ChunkFetcher = ContentAddressableStorage(nil)

// ContentAddressableStorage implements the ParametersFetcher interface.
var _ cdc.ParametersFetcher = ContentAddressableStorage(nil)
