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
// checksum. XXH64 is a modern non cryptographic hash function designed
// to be extremely fast.
func NewXXH64SuffixCoder() Coder[[]byte, []byte] {
	return &xxh64SuffixCoder{}
}

func (xxh64SuffixCoder) Encode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	hash := xxhash.Sum64(data)
	return binary.LittleEndian.AppendUint64(data, hash), nil
}

func (xxh64SuffixCoder) Decode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	if len(data) < 8 {
		return nil, status.Errorf(codes.InvalidArgument, "Data too short to contain XXH64 suffix")
	}

	suffixIndex := len(data) - 8
	payload := data[:suffixIndex]
	expectedHash := binary.LittleEndian.Uint64(data[suffixIndex:])

	actualHash := xxhash.Sum64(payload)
	if actualHash != expectedHash {
		return nil, status.Errorf(codes.Internal, "XXH64 checksum mismatch: expected %016x, got %016x", expectedHash, actualHash)
	}

	return payload, nil
}
