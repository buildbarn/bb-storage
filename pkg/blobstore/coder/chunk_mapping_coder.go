package coder

import (
	"encoding/binary"
	"encoding/hex"
	"math"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkMappingCoder struct {
	prevalidated bool
}

// NewChunkMappingCoder returns a Coder that can encode and decode a
// chunk.Mapping into a tightly packed binary format.
//
// The parameter prevalidated determines if the chunk mapping should be
// marked as prevalidated when decoded from its binary format. A
// prevalidated chunk mapping is a chunk mapping which has already been
// determined to compose the digest which it is being addressed by (see
// chunk_mapping_validating_blob_access.go for the logic to validate a
// chunk mapping).
//
// Binary format:
//
//	struct ChunkMapping {
//	  uint32_t sizes[count];            // The size of each chunk in order.
//	  uint8_t hashes[count][hash_len];  // A contiguous array of hashes.
//	}
func NewChunkMappingCoder(prevalidated bool) Coder[chunk.Mapping, []byte] {
	return &chunkMappingCoder{prevalidated: prevalidated}
}

func (chunkMappingCoder) Encode(chunkMapping chunk.Mapping, d digest.Digest) ([]byte, error) {
	hashLen := uint32(len(d.GetHashBytes()))
	count := uint32(chunkMapping.Length())
	size := 4*count + hashLen*count
	data := make([]byte, size)
	offset := 0
	for i := range chunkMapping.Length() {
		chunkDigest := chunkMapping.GetDigestAtIndex(i)
		if chunkDigest.GetSizeBytes() > math.MaxUint32 {
			return nil, status.Errorf(codes.InvalidArgument, "Attempted to serialize digest of size %d but we can only encode up to size %d", chunkDigest.GetSizeBytes(), uint32(math.MaxUint32))
		}
		binary.LittleEndian.PutUint32(data[offset:], uint32(chunkDigest.GetSizeBytes()))
		offset += 4
	}
	for i := range chunkMapping.Length() {
		bytes := chunkMapping.GetDigestAtIndex(i).GetHashBytes()
		copy(data[offset:], bytes)
		offset += len(bytes)
	}
	return data, nil
}

func (c *chunkMappingCoder) Decode(data []byte, d digest.Digest) (chunk.Mapping, error) {
	hashLen := uint32(len(d.GetHashBytes()))
	sizeBytes := uint32(len(data))
	if sizeBytes%(hashLen+4) != 0 {
		return chunk.Mapping{}, status.Error(codes.InvalidArgument, "Data does not add up to a whole number of digests")
	}
	count := sizeBytes / (hashLen + 4)
	digestFunction := d.GetDigestFunction()
	chunkDigests := make([]digest.Digest, 0, count)
	for i := range count {
		size := int64(binary.LittleEndian.Uint32(data[i*4:]))
		stringHash := hex.EncodeToString(data[4*count+i*hashLen : 4*count+(i+1)*hashLen])
		chunkDigest, err := digestFunction.NewDigest(stringHash, size)
		if err != nil {
			return chunk.Mapping{}, err
		}
		chunkDigests = append(chunkDigests, chunkDigest)
	}
	return chunk.NewMappingFromDigests(chunkDigests, uint64(d.GetSizeBytes()), c.prevalidated)
}
