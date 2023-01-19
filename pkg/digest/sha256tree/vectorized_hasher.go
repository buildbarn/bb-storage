//go:build amd64

package sha256tree

// vectorizedChunksSizeBytes is the number of bytes of data that
// hashChunksVectorized() can process at once.
const vectorizedChunksSizeBytes = vectorizedChunksPerCycle * maximumChunkSizeBytes

// Methods that need to be written in assembly, using CPU vectorization
// extensions.

//go:noescape
func hashChunksVectorized(input *[vectorizedChunksSizeBytes]byte, output *[Size / 4][vectorizedChunksPerCycle]uint32)

//go:noescape
func hashParentsVectorized(left, right, output *[Size / 4][vectorizedParentsPerCycle]uint32)

type vectorizedHasher struct {
	// Chunks of data that are being ingested.
	chunks     [vectorizedChunksSizeBytes]byte
	chunksSize int

	// Left/right children whose parents still need to be computed.
	left          [Size / 4][vectorizedParentsPerCycle]uint32
	right         [Size / 4][vectorizedParentsPerCycle]uint32
	parentHeights [vectorizedParentsPerCycle]int
	pending       int

	// Parents that still need to be paired up to a sibling.
	stack           [][Size / 4]uint32
	stackOccupation uint64
}

// newVectorizedHasher creates a new SHA256TREE hasher that processes
// data in parallel (i.e., using SIMD to process multiple chunks of data
// at once).
func newVectorizedHasher(expectedSizeBytes int64) *vectorizedHasher {
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
	return &vectorizedHasher{
		stack: stack,
	}
}

func (h *vectorizedHasher) Write(b []byte) (int, error) {
	nWritten := len(b)
	for {
		// Store more data within the current set of chunks.
		n := copy(h.chunks[h.chunksSize:], b)
		b = b[n:]
		h.chunksSize += n
		if len(b) == 0 {
			return nWritten, nil
		}

		// Read enough chunks of data. Compute the chunk
		// chaining values for each of the chunks in parallel
		// and store them in the Merkle tree.
		var chunkChainingValues [Size / 4][vectorizedChunksPerCycle]uint32
		hashChunksVectorized(&h.chunks, &chunkChainingValues)
		for chunk := 0; chunk < vectorizedChunksPerCycle; chunk += 2 {
			// Schedule the computation of parent chaining values
			// for two consecutive chunk chaining values.
			for i := 0; i < Size/4; i++ {
				h.left[i][h.pending] = chunkChainingValues[i][chunk]
				h.right[i][h.pending] = chunkChainingValues[i][chunk+1]
			}
			h.parentHeights[h.pending] = 0
			h.pending++

			// If we have computed a sufficient number of
			// left/right pairs, we can compute parent
			// chaining values in parallel.
			for h.pending == vectorizedParentsPerCycle {
				h.flushPending()
			}
		}
		h.chunksSize = 0
	}
}

func (h *vectorizedHasher) flushPending() {
	// Compute parent chaining values.
	var parentChainingValues [Size / 4][vectorizedParentsPerCycle]uint32
	hashParentsVectorized(&h.left, &h.right, &parentChainingValues)

	// Reinsert parent chaining values into the queue.
	pending := h.pending
	h.pending = 0
	for parent := 0; parent < pending; parent++ {
		h.appendParentChainingValue(&parentChainingValues, parent, h.parentHeights[parent])
	}
}

func (h *vectorizedHasher) appendParentChainingValue(chainingValues *[Size / 4][vectorizedChunksPerCycle]uint32, index, height int) {
	mask := uint64(1) << height
	if h.stackOccupation&mask == 0 {
		// Observing a left child. Save it, so that we can later
		// pair it up with the right child.
		if len(h.stack) == height {
			h.stack = append(h.stack, [Size / 4]uint32{})
		}
		for i := 0; i < Size/4; i++ {
			h.stack[height][i] = chainingValues[i][index]
		}
	} else {
		// Observing a right child. Load its left sibling and
		// prepare them for compression.
		for i := 0; i < Size/4; i++ {
			h.left[i][h.pending] = h.stack[height][i]
			h.right[i][h.pending] = chainingValues[i][index]
		}
		h.parentHeights[h.pending] = height + 1
		h.pending++
	}
	h.stackOccupation ^= mask
}

func (h *vectorizedHasher) Sum(b []byte) []byte {
	for h.pending > 0 {
		h.flushPending()
	}

	// Compute the hash of the final data, using a non-vectorized
	// hasher. Only that implementation is capable of dealing with
	// variable length data.
	tailHasher := newHasher(int64(h.chunksSize))
	tailHasher.Write(h.chunks[:h.chunksSize])
	if h.stackOccupation == 0 {
		return tailHasher.Sum(b)
	}

	// Compute root of the Merkle tree.
	var chainingValue [Size / 4]uint32
	tailHasher.getRootChainingValue(&chainingValue)
	for height := 0; height < len(h.stack); height++ {
		if h.stackOccupation&(uint64(1)<<height) != 0 {
			compressParent(&h.stack[height], &chainingValue, &chainingValue)
		}
	}
	return chainingValueToSum(&chainingValue, b)
}

func (h *vectorizedHasher) Reset() {
	h.chunksSize = 0
	h.pending = 0
	h.stackOccupation = 0
}

func (h *vectorizedHasher) Size() int {
	return Size
}

func (h *vectorizedHasher) BlockSize() int {
	return vectorizedChunksSizeBytes
}
