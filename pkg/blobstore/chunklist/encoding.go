package chunklist

import (
	"bytes"

	"github.com/buildbarn/bb-storage/pkg/digest"
	chunklist_pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// EncodeToBinary converts a ChunkList into a compact binary
// representation. Each digest is encoded using
// digest.Digest.GetCompactBinary().
func EncodeToBinary(chunkDigests []digest.Digest) []byte {
	var buf bytes.Buffer
	for _, d := range chunkDigests {
		buf.Write(d.GetCompactBinary())
	}
	return buf.Bytes()
}

// DecodeFromBinary parses a compact binary representation of a chunk
// list back into a ChunkList.
func DecodeFromBinary(data []byte, instanceName digest.InstanceName) (ChunkList, error) {
	r := bytes.NewReader(data)
	var cl ChunkList
	var offset uint64
	for r.Len() > 0 {
		d, err := instanceName.NewDigestFromCompactBinary(r)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse binary coded digest in chunk list")
		}
		cl = append(cl, Entry{
			Offset: offset,
			Digest: d,
		})
		offset += uint64(d.GetSizeBytes())
	}
	return cl, nil
}

// ToProto converts a ChunkList to the protobuf storage format.
func ToProto(cl ChunkList) *chunklist_pb.ChunkList {
	digests := make([]digest.Digest, 0, len(cl))
	for _, entry := range cl {
		digests = append(digests, entry.Digest)
	}
	return &chunklist_pb.ChunkList{
		Data: EncodeToBinary(digests),
	}
}

// NewChunkListFromProto converts a protobuf ChunkList into a ChunkList.
func NewChunkListFromProto(proto *chunklist_pb.ChunkList, instanceName digest.InstanceName) (ChunkList, error) {
	return DecodeFromBinary(proto.Data, instanceName)
}
