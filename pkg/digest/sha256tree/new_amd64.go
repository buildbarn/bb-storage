//go:build amd64

package sha256tree

import (
	"hash"

	"golang.org/x/sys/cpu"
)

// AVX2 has 256-bit registers, meaning we can process eight chunks in
// parallel.
const (
	vectorizedChunksPerCycle  = 8
	vectorizedParentsPerCycle = 8
)

// New creates a SHA256TREE hasher. Depending on the expected size,
// either a vectorized or a scalar hasher is created.
func New(expectedSizeBytes int64) hash.Hash {
	if expectedSizeBytes > vectorizedChunksSizeBytes && cpu.X86.HasAVX2 {
		return newVectorizedHasher(expectedSizeBytes)
	}
	return newHasher(expectedSizeBytes)
}
