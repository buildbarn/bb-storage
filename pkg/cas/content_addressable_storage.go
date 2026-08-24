package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type contentAddressableStorage struct {
	chunkStorage         blobstore.BlobAccess
	chunkListFetcher     chunklist.Fetcher
	cdcParametersFetcher cdc.ParametersFetcher
	chunkListStorage     blobstore.BlobAccess
	digestKeyFormat      digest.KeyFormat
}

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

// NewContentAddressableStorage creates an interface towards the Content
// Addressable Storage (CAS) that allows access to arbitrary sized
// objects from the chunks described in the chunk list.
func NewContentAddressableStorage(chunkStorage, chunkListStorage blobstore.BlobAccess, chunkListFetcher chunklist.Fetcher, cdcParametersFetcher cdc.ParametersFetcher, digestKeyFormat digest.KeyFormat) ContentAddressableStorage {
	return &contentAddressableStorage{
		chunkStorage:         chunkStorage,
		chunkListStorage:     chunkListStorage,
		chunkListFetcher:     chunkListFetcher,
		cdcParametersFetcher: cdcParametersFetcher,
		digestKeyFormat:      digestKeyFormat,
	}
}

func (cas *contentAddressableStorage) GetDigestKeyFormat() digest.KeyFormat {
	return cas.digestKeyFormat
}

func (cas *contentAddressableStorage) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (cdc.Parameters, error) {
	return cas.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
}

func (cas *contentAddressableStorage) FetchChunk(ctx context.Context, d digest.Digest) ([]byte, error) {
	return cas.chunkStorage.Get(ctx, d).ToByteSlice(int(d.GetSizeBytes()))
}

func (cas *contentAddressableStorage) PutChunk(ctx context.Context, d digest.Digest, data []byte) error {
	return cas.chunkStorage.Put(ctx, d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided))
}

func (cas *contentAddressableStorage) GetManifest(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	return cas.chunkListFetcher.FetchChunkList(ctx, d)
}

func (cas *contentAddressableStorage) PutManifest(ctx context.Context, d digest.Digest, manifest chunklist.ChunkList) error {
	b := buffer.NewProtoBufferFromProto(chunklist.ToProto(manifest), buffer.UserProvided)
	if err := cas.chunkListStorage.Put(ctx, d, b); err != nil {
		return util.StatusWrap(err, "Could not save chunk list for blob")
	}
	return nil
}

func (cas *contentAddressableStorage) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	digestSets := digests.PartitionByInstanceName()
	missings := make([]digest.Set, 0, len(digestSets))
	for _, digestSet := range digestSets {
		// PartitionByInstanceNames guarantees non empty sets.
		missing, err := cas.findMissingFromInstance(ctx, digestSet.Items()[0].GetInstanceName(), digestSet)
		if err != nil {
			return digest.EmptySet, err
		}
		missings = append(missings, missing)
	}
	return digest.GetUnion(missings), nil
}

func (cas *contentAddressableStorage) findMissingFromInstance(ctx context.Context, instanceName digest.InstanceName, digests digest.Set) (digest.Set, error) {
	params, err := cas.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return digest.EmptySet, err
	}
	smallDigests := digest.NewSetBuilder(digests.Length())
	largeDigests := digest.NewSetBuilder(digests.Length())
	for _, d := range digests.Items() {
		if IsSingleChunk(params, d) {
			smallDigests.Add(d)
		} else {
			largeDigests.Add(d)
		}
	}
	smallMissing, err := cas.chunkStorage.FindMissing(ctx, smallDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	largeMissing, err := cas.chunkListStorage.FindMissing(ctx, largeDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion([]digest.Set{smallMissing, largeMissing}), nil
}
