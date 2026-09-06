package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	chunklist_pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type blobAccessChunkListFetcher struct {
	chunkListStorage        BlobAccess
	maximumMessageSizeBytes int
}

// NewBlobAccessChunkListFetcher creates a ChunkListFetcher that reads
// and decodes chunk lists from the provided BlobAccess.
func NewBlobAccessChunkListFetcher(chunkListStorage BlobAccess, maximumMessageSizeBytes int) chunklist.Fetcher {
	return &blobAccessChunkListFetcher{
		chunkListStorage:        chunkListStorage,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (f *blobAccessChunkListFetcher) FetchChunkList(ctx context.Context, d digest.Digest) (chunklist.ChunkList, error) {
	msg, err := f.chunkListStorage.Get(ctx, d).ToProto(&chunklist_pb.ChunkList{}, f.maximumMessageSizeBytes)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to fetch chunk list")
	}
	proto := msg.(*chunklist_pb.ChunkList)
	return chunklist.NewChunkListFromProto(proto, d.GetInstanceName())
}
