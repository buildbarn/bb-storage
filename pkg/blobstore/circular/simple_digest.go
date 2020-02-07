package circular

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// simpleDigest is the on-disk format for digests that the circular
// storage backend uses.
//
// Digests are encoded by storing the hash, followed by the size. Enough
// space is left for a SHA-256 sum.
type simpleDigest [sha256.Size + 8]byte

// NewSimpleDigest converts a Digest to a simpleDigest.
func newSimpleDigest(digest digest.Digest) simpleDigest {
	var sd simpleDigest
	copy(sd[:], digest.GetHashBytes())
	binary.LittleEndian.PutUint32(sd[sha256.Size:], uint32(digest.GetSizeBytes()))
	return sd
}
