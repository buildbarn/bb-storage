package coder

import (
	"crypto/sha256"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sha256SuffixCoder struct{}

// NewSHA256SuffixCoder creates a Coder that appends a 32-byte SHA-256
// digest. The digest is computed over both the data and the parent
// digest. This allows the digest to be used to verify that the bytes
// that are being read belong to the object for which they were
// originally written.
func NewSHA256SuffixCoder() Coder[[]byte, []byte] {
	return &sha256SuffixCoder{}
}

func (sha256SuffixCoder) Encode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	hasher := sha256.New()
	hasher.Write(parentDigest.GetHashBytes())
	hasher.Write(data)
	return hasher.Sum(data), nil
}

func (sha256SuffixCoder) Decode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	if len(data) < sha256.Size {
		return nil, status.Errorf(codes.InvalidArgument, "Data too short to contain SHA-256 suffix")
	}

	suffixIndex := len(data) - sha256.Size
	payload := data[:suffixIndex]
	expectedHash := data[suffixIndex:]

	hasher := sha256.New()
	hasher.Write(parentDigest.GetHashBytes())
	hasher.Write(payload)
	actualHash := hasher.Sum(nil)
	for i := range actualHash {
		if actualHash[i] != expectedHash[i] {
			return nil, status.Errorf(codes.InvalidArgument, "SHA-256 checksum mismatch")
		}
	}

	return payload, nil
}
