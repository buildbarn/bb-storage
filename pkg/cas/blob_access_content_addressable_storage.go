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
	chunkListStorage     blobstore.BlobAccess
	chunkListFetcher     chunklist.Fetcher
	cdcParametersFetcher cdc.ParametersFetcher
	digestKeyFormat      digest.KeyFormat
}

// NewBlobAccessContentAddressableStorage creates an interface towards
// the Content Addressable Storage (CAS) from a pair of BlobAccess.
func NewBlobAccessContentAddressableStorage(chunkStorage, chunkListStorage blobstore.BlobAccess, chunkListFetcher chunklist.Fetcher, cdcParametersFetcher cdc.ParametersFetcher, digestKeyFormat digest.KeyFormat) ContentAddressableStorage {
	return &contentAddressableStorage{
		chunkStorage:         chunkStorage,
		chunkListStorage:     chunkListStorage,
		chunkListFetcher:     chunkListFetcher,
		cdcParametersFetcher: cdcParametersFetcher,
		digestKeyFormat:      digestKeyFormat,
	}
}

func (s *contentAddressableStorage) GetDigestKeyFormat() digest.KeyFormat {
	return s.digestKeyFormat
}

func (s *contentAddressableStorage) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (cdc.Parameters, error) {
	return s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
}

func (s *contentAddressableStorage) FetchChunk(ctx context.Context, d digest.Digest) ([]byte, error) {
	return s.chunkStorage.Get(ctx, d).ToByteSlice(int(d.GetSizeBytes()))
}

func (s *contentAddressableStorage) PutChunk(ctx context.Context, d digest.Digest, data []byte) error {
	return s.chunkStorage.Put(ctx, d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided))
}

func (s *contentAddressableStorage) GetManifest(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	return s.chunkListFetcher.FetchChunkList(ctx, d)
}

func (s *contentAddressableStorage) PutManifest(ctx context.Context, d digest.Digest, manifest chunklist.ChunkList) error {
	b := buffer.NewProtoBufferFromProto(chunklist.ToProto(manifest), buffer.UserProvided)
	if err := s.chunkListStorage.Put(ctx, d, b); err != nil {
		return util.StatusWrap(err, "Could not save chunk list for blob")
	}
	return nil
}

func (s *contentAddressableStorage) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	digestSets := digests.PartitionByInstanceName()
	missings := make([]digest.Set, 0, len(digestSets))
	for _, digestSet := range digestSets {
		// PartitionByInstanceNames guarantees non empty sets.
		missing, err := s.findMissingFromInstance(ctx, digestSet.Items()[0].GetInstanceName(), digestSet)
		if err != nil {
			return digest.EmptySet, err
		}
		missings = append(missings, missing)
	}
	return digest.GetUnion(missings), nil
}

func (s *contentAddressableStorage) findMissingFromInstance(ctx context.Context, instanceName digest.InstanceName, digests digest.Set) (digest.Set, error) {
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
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
	smallMissing, err := s.chunkStorage.FindMissing(ctx, smallDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	largeMissing, err := s.chunkListStorage.FindMissing(ctx, largeDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion([]digest.Set{smallMissing, largeMissing}), nil
}
