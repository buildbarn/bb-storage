package coder

import (
	"encoding/binary"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type xxh64SuffixCoder struct{}

// NewXXH64SuffixCoder creates a Coder that appends an 8-byte XXH64
// checksum. The checksum is computed over both the data and the parent
// digest. This allows the checksum to be used to verify that the bytes
// that are being read belong to the object for which they were
// originally written.
func NewXXH64SuffixCoder() Coder[[]byte, []byte] {
	return &xxh64SuffixCoder{}
}

func (xxh64SuffixCoder) encodeHash(data []byte, d digest.Digest) uint64 {
	hasher := xxhash.New()
	hasher.Write(d.GetHashBytes())
	hasher.Write(data)
	return hasher.Sum64()
}

func (c xxh64SuffixCoder) Encode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	hash := c.encodeHash(data, parentDigest)
	return binary.LittleEndian.AppendUint64(data, hash), nil
}

func (c xxh64SuffixCoder) Decode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	if len(data) < 8 {
		return nil, status.Errorf(codes.InvalidArgument, "Data too short to contain XXH64 suffix")
	}

	suffixIndex := len(data) - 8
	payload := data[:suffixIndex]
	expectedHash := binary.LittleEndian.Uint64(data[suffixIndex:])

	actualHash := c.encodeHash(payload, parentDigest)
	if actualHash != expectedHash {
		return nil, status.Errorf(codes.Internal, "XXH64 checksum mismatch: expected %016x, got %016x", expectedHash, actualHash)
	}

	return payload, nil
}
