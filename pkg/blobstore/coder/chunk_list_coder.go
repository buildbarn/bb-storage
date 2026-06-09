package coder

import (
	"encoding/binary"
	"encoding/hex"
	"math"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkListCoder struct {
	prevalidated bool
}

// NewChunkListCoder returns a Coder that can encode and decode a
// chunklist.ChunkList into an efficient binary format.
//
// The binary schema uses a Struct-of-Arrays (SoA) layout to group sizes
// together which maximizes compressibility but does not itself compress
// or do integrity checks of the data.
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
//	  uint8_t version;                  // The version, currently 0x00.
//	  uint8_t digest_function;          // The REv2 digest function enum.
//	  uint32_t count:                   // The number of chunks.
//	  uint32_t sizes[count];            // The size of each chunk in order.
//	  uint8_t hashes[count][hash_len];  // A contiguous array of hashes.
//	}
func NewChunkListCoder(prevalidated bool) Coder[chunklist.ChunkList, []byte] {
	return &chunkListCoder{prevalidated: prevalidated}
}

func (chunkListCoder) Encode(chunkList chunklist.ChunkList, d digest.Digest) ([]byte, error) {
	hashLen := uint32(len(d.GetHashBytes()))
	count := uint32(len(chunkList.Digests))
	size := 1 + 1 + 4 + 4*count + hashLen*count
	data := make([]byte, size)
	// version
	data[0] = 0x00
	// digest_function
	data[1] = byte(d.GetDigestFunction().GetEnumValue())
	// count
	binary.LittleEndian.PutUint32(data[2:], count)
	// sizes[count]
	offset := 6
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

func (c *chunkListCoder) Decode(data []byte, d digest.Digest) (chunklist.ChunkList, error) {
	hashLen := uint32(len(d.GetHashBytes()))
	sizeBytes := len(data)
	if sizeBytes < 6 {
		return chunklist.ChunkList{}, status.Error(codes.InvalidArgument, "Data is less than 6 bytes which no valid chunk list can be")
	}
	if data[0] != 0x00 {
		return chunklist.ChunkList{}, status.Errorf(codes.InvalidArgument, "Unknown version %d", data[0])
	}
	digestFunction := d.GetDigestFunction()
	expectedDigestEnum := digestFunction.GetEnumValue()
	if data[1] != byte(expectedDigestEnum) {
		storedDigestStr := remoteexecution.DigestFunction_Value(data[1]).String()
		expectedDigestStr := expectedDigestEnum.String()

		return chunklist.ChunkList{}, status.Errorf(
			codes.InvalidArgument,
			"Digest function in storage %s does not match expected digest function %s",
			storedDigestStr,
			expectedDigestStr,
		)
	}
	count := binary.LittleEndian.Uint32(data[2:6])
	expectedSizeBytes := int(count)*4 + int(count)*int(hashLen) + 6
	if expectedSizeBytes != sizeBytes {
		return chunklist.ChunkList{}, status.Errorf(codes.InvalidArgument, "Expected binary representation to be %d bytes but it was %d bytes", expectedSizeBytes, sizeBytes)
	}
	ret := chunklist.ChunkList{
		Offsets:   make([]uint64, count),
		Digests:   make([]digest.Digest, count),
		Validated: c.prevalidated,
	}
	for i := uint32(0); i < count; i++ {
		sizeBytes := int64(binary.LittleEndian.Uint32(data[6+i*4:]))
		stringHash := hex.EncodeToString(data[6+4*count+i*hashLen : 6+4*count+(i+1)*hashLen])
		chunkDigest, err := digestFunction.NewDigest(stringHash, sizeBytes)
		if err != nil {
			return chunklist.ChunkList{}, err
		}
		ret.Digests[i] = chunkDigest
	}
	offset := uint64(0)
	for i, d := range ret.Digests {
		ret.Offsets[i] = offset
		offset += uint64(d.GetSizeBytes())
	}
	return ret, nil
}
