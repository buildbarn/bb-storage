package local

import (
	"crypto/sha256"
)

// CompactDigest is a binary representation of digest.Digest that is
// used as the key type by DigestLocationMap. It is not possible to
// convert CompactDigestLocation back to a digest.Digest.
type CompactDigest [sha256.Size]byte

// NewCompactDigest creates a new CompactDigest based on a key that is
// returned by digest.Digest.GetKey().
func NewCompactDigest(key string) CompactDigest {
	return sha256.Sum256([]byte(key))
}

// DigestLocationMap is equivalent to a map[CompactDigest]Location. It is
// used by LocalBlobAccess to track where blobs are stored, so that they
// may be accessed. Implementations are permitted to discard entries
// for outdated locations during lookups/insertions using the provided
// validator.
type DigestLocationMap interface {
	Get(digest CompactDigest, validator *LocationValidator) (Location, error)
	Put(digest CompactDigest, validator *LocationValidator, location Location) error
}
