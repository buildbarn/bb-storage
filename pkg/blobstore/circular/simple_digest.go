package circular

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// simpleDigest is the on-disk format for digests that the circular
// storage backend uses.
//
// Digests are encoded by storing the hash, followed by the size. Enough
// space is left for a SHA-256 sum.
type simpleDigest [sha256.Size + 8]byte

// NewSimpleDigest converts a Digest to a simpleDigest.
func newSimpleDigest(digest *util.Digest) simpleDigest {
	var sd simpleDigest
	copy(sd[:], digest.GetHashBytes())
	binary.LittleEndian.PutUint32(sd[sha256.Size:], uint32(digest.GetSizeBytes()))
	return sd
}
