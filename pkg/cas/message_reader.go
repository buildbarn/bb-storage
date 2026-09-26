package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/proto"
)

type messageReader[T any, TPtr interface {
	*T
	proto.Message
}] struct {
	chunkBytesReader        reader.Reader[[]byte]
	chunkMappingFetcher     chunk.MappingFetcher
	cdcParametersFetcher    capabilities.CDCParametersFetcher
	maximumMessageSizeBytes int
}

// NewMessageReader creates a Reader that reads a proto message from the
// Content Addressable Storage, up to maximumMessageSizeBytes large.
func NewMessageReader[T any, TPtr interface {
	*T
	proto.Message
}](chunkBytesReader reader.Reader[[]byte], chunkMappingFetcher chunk.MappingFetcher, cdcParametersFetcher capabilities.CDCParametersFetcher, maximumMessageSizeBytes int) reader.Reader[TPtr] {
	return &messageReader[T, TPtr]{
		chunkBytesReader:        chunkBytesReader,
		chunkMappingFetcher:     chunkMappingFetcher,
		cdcParametersFetcher:    cdcParametersFetcher,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (mr *messageReader[T, TPtr]) Read(ctx context.Context, d digest.Digest) (TPtr, error) {
	params, err := mr.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch CDC parameters")
	}
	msg := TPtr(new(T))
	bytes, err := GetBytes(ctx, mr.chunkBytesReader, mr.chunkMappingFetcher, params, d, int64(mr.maximumMessageSizeBytes))
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(bytes, msg)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to unmarshal message")
	}
	return msg, nil
}
