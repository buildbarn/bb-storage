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

type chunkListCoder struct {
	prevalidated bool
}

// NewChunkListCoder returns a Coder that can encode and decode a
// chunk.List into a tightly packed binary format.
//
// The parameter prevalidated determines if the chunk list should be
// marked as prevalidated when decoded from its binary format. A
// prevalidated chunk list is a chunk list which has already been
// determined to compose the digest which it is being addressed by (see
// chunk_list_validating_blob_access.go for the logic to validate a
// chunk list).
//
// Binary format:
//
//	struct ChunkList {
//	  uint32_t sizes[count];            // The size of each chunk in order.
//	  uint8_t hashes[count][hash_len];  // A contiguous array of hashes.
//	}
func NewChunkListCoder(prevalidated bool) Coder[chunk.List, []byte] {
	return &chunkListCoder{prevalidated: prevalidated}
}

func (chunkListCoder) Encode(chunkList chunk.List, d digest.Digest) ([]byte, error) {
	hashLen := uint32(len(d.GetHashBytes()))
	count := uint32(len(chunkList.Digests))
	size := 4*count + hashLen*count
	data := make([]byte, size)
	offset := 0
	for _, digest := range chunkList.Digests {
		if digest.GetSizeBytes() > math.MaxUint32 {
			return nil, status.Errorf(codes.Internal, "Attempted to serialie digest of size %d but we can only encode up to size %d", digest.GetSizeBytes(), uint32(math.MaxUint32))
		}
		binary.LittleEndian.PutUint32(data[offset:], uint32(digest.GetSizeBytes()))
		offset += 4
	}
	for _, digest := range chunkList.Digests {
		bytes := digest.GetHashBytes()
		copy(data[offset:], bytes)
		offset += len(bytes)
	}
	return data, nil
}

func (c *chunkListCoder) Decode(data []byte, d digest.Digest) (chunk.List, error) {
	hashLen := uint32(len(d.GetHashBytes()))
	sizeBytes := uint32(len(data))
	if sizeBytes%(hashLen+4) != 0 {
		return chunk.List{}, status.Error(codes.Internal, "Data does not add up to a whole number of digests")
	}
	count := sizeBytes / (hashLen + 4)
	digestFunction := d.GetDigestFunction()
	chunkDigests := make([]digest.Digest, 0, count)
	for i := range count {
		size := int64(binary.LittleEndian.Uint32(data[i*4:]))
		stringHash := hex.EncodeToString(data[4*count+i*hashLen : 4*count+(i+1)*hashLen])
		chunkDigest, err := digestFunction.NewDigest(stringHash, size)
		if err != nil {
			return chunk.List{}, err
		}
		if size == 0 {
			// Zero length chunks carry no data, so they may simply be
			// removed from the resulting list.
			continue
		}
		chunkDigests = append(chunkDigests, chunkDigest)
	}
	// Chunk lists only exist for blobs composed of at least two
	// chunks. Blobs of fewer chunks have no chunk list in storage,
	// so lists of fewer chunks are invalid.
	if len(chunkDigests) < 2 {
		return chunk.List{}, status.Error(codes.Internal, "Chunk list contains fewer than two chunks")
	}
	return chunk.NewList(chunkDigests, uint64(d.GetSizeBytes()), c.prevalidated)
}
