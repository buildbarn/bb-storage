//go:build !amd64

package sha256tree

import (
	"hash"
)

// New creates a new SHA256TREE hasher.
func New(expectedSizeBytes int64) hash.Hash {
	// No vectorization support available.
	return newHasher(expectedSizeBytes)
}
