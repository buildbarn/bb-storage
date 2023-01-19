package sha256tree

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

// Size of SHA256TREE hashes.
const Size = sha256.Size

// maximumChunkSizeBytes is the maximum number of bytes of input for
// which SHA256TREE still behaves identically to plain SHA-256.
const maximumChunkSizeBytes = 1024

type hasher struct {
	chunkHasher    hash.Hash
	chunkRemaining int

	stack      [][Size / 4]uint32
	totalNodes uint64
}

// newHasher creates a new SHA256TREE hasher that processes data
// sequentially (i.e., without using SIMD to process multiple chunks of
// data at once).
func newHasher(expectedSizeBytes int64) *hasher {
	// Preallocate stack that is sufficiently large for the expected
	// amount of data.
	var stack [][Size / 4]uint32
	if expectedSizeBytes > maximumChunkSizeBytes {
		stackSize := 0
		for expectedSizeBytes > maximumChunkSizeBytes {
			stackSize++
			expectedSizeBytes >>= 1
		}
		stack = make([][Size / 4]uint32, 0, stackSize)
	}
	return &hasher{
		chunkHasher:    sha256.New(),
		chunkRemaining: maximumChunkSizeBytes,
		stack:          stack,
	}
}

// getCurrentChainingValue returns the hash of the current chunk.
func (h *hasher) getCurrentChainingValue(chainingValue *[Size / 4]uint32) {
	var sum [Size]byte
	h.chunkHasher.Sum(sum[:0])
	for i := range chainingValue {
		chainingValue[i] = binary.BigEndian.Uint32(sum[i*4:])
	}
}

// getRootChainingValue returns the hash of all of the data written so far.
func (h *hasher) getRootChainingValue(chainingValue *[Size / 4]uint32) {
	h.getCurrentChainingValue(chainingValue)
	for i := len(h.stack) - 1; i >= 0; i-- {
		compressParent(&h.stack[i], chainingValue, chainingValue)
	}
}

func (h *hasher) Write(b []byte) (int, error) {
	nTotal := 0
	for {
		nWrite := len(b)
		if nWrite == 0 {
			return nTotal, nil
		}

		if h.chunkRemaining == 0 {
			// Current chunk is full. Insert its hash into
			// the stack, potentially combining it with
			// existing hashes.
			var chainingValue [Size / 4]uint32
			h.getCurrentChainingValue(&chainingValue)
			for totalNodes := h.totalNodes; totalNodes&1 != 0; totalNodes >>= 1 {
				compressParent(&h.stack[len(h.stack)-1], &chainingValue, &chainingValue)
				h.stack = h.stack[:len(h.stack)-1]
			}

			// Start reading a new chunk.
			h.resetChunkHasher()
			h.stack = append(h.stack, chainingValue)
			h.totalNodes++
		}

		// Ingest data.
		if nWrite > h.chunkRemaining {
			nWrite = h.chunkRemaining
		}
		nWritten, err := h.chunkHasher.Write(b[:nWrite])
		h.chunkRemaining -= nWritten
		nTotal += nWritten
		if err != nil {
			return nTotal, err
		}
		b = b[nWritten:]
	}
}

// chainingValueToSum appends a chaining value to the hash output, as
// needs to be done by Hash.Sum().
func chainingValueToSum(chainingValue *[Size / 4]uint32, b []byte) []byte {
	// Convert chaining value to bytes.
	l := len(b)
	b = append(b, make([]byte, Size)...)
	out := b[l:]
	for _, v := range chainingValue {
		if len(out) < 4 {
			var x [4]byte
			binary.BigEndian.PutUint32(x[:], v)
			copy(out, x[:])
			return b
		}
		binary.BigEndian.PutUint32(out, v)
		out = out[4:]
	}
	return b
}

func (h *hasher) Sum(b []byte) []byte {
	if len(h.stack) == 0 {
		// Object is smaller than a single chunk, meaning there
		// are no parent nodes. Simply call into the underlying
		// hasher to obtain the hash.
		return h.chunkHasher.Sum(b)
	}

	var chainingValue [Size / 4]uint32
	h.getRootChainingValue(&chainingValue)
	return chainingValueToSum(&chainingValue, b)
}

func (h *hasher) resetChunkHasher() {
	h.chunkHasher.Reset()
	h.chunkRemaining = maximumChunkSizeBytes
}

func (h *hasher) Reset() {
	h.resetChunkHasher()
	h.stack = h.stack[:0]
	h.totalNodes = 0
}

func (h *hasher) Size() int {
	return Size
}

func (h *hasher) BlockSize() int {
	return h.chunkHasher.BlockSize()
}
