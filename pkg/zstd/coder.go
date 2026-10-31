package zstd

import (
	"runtime"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/klauspost/compress/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	coderEncoder *zstd.Encoder
	coderDecoder *zstd.Decoder
)

func init() {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(runtime.NumCPU()))
	if err != nil {
		panic("failed to create ZSTD encoder: " + err.Error())
	}
	decoder, err := zstd.NewReader(
		nil,
		zstd.WithDecoderConcurrency(runtime.NumCPU()),
		zstd.WithDecodeAllCapLimit(true),
		zstd.WithDecoderLowmem(true),
	)
	if err != nil {
		panic("failed to create ZSTD decoder: " + err.Error())
	}
	coderEncoder = encoder
	coderDecoder = decoder
}

// 4MiB scratch size should be enough for the vast majority of blobs.
// The cost of exceeding the scratch size is extra allocations as the
// zstd encoder doubles the buffer size when it needs to grow the
// buffer.
const scratchSize = 4 << 20

// A threshold below which compression will not be performed. Encoding
// will still return a byte slice which is fully valid ZSTD data for
// smaller inputs.
const minimumCompressionSizeBytes = 512

var scratchPool = sync.Pool{
	New: func() any {
		scratch := make([]byte, 0, scratchSize)
		return &scratch
	},
}

// Encoding is bounded by the number of CPUs, matching the concurrency
// of the encoder instance itself. This bounds the number of scratch
// buffers that can be checked out of the pool at any point in time,
// regardless of the number of concurrent callers.
var encodeSlots = func() chan struct{} {
	slots := make(chan struct{}, runtime.NumCPU())
	for range runtime.NumCPU() {
		slots <- struct{}{}
	}
	return slots
}()

// Encode compresses the provided data into a newly allocated byte slice
// that holds exactly the compressed representation.
//
// The returned byte slice is tight, meaning that its capacity is equal
// to its length. It may be retained freely.
func Encode(data []byte) []byte {
	if len(data) < minimumCompressionSizeBytes {
		// Data that is too small to be worth compressing is stored as
		// a ZSTD frame containing a single uncompressed block:

		result := make([]byte, 9+len(data))
		// Magic number, frame header descriptor (no frame content size,
		// no checksums, no dictionary) and window descriptor (minimum
		// window size).
		copy(result, "\x28\xb5\x2f\xfd\x00\x00")
		// Block header: last block flag in bit 0, block type
		// uncompressed in bits 1-2, block size in bits 3-23.
		blockHeader := uint32(len(data))<<3 | 1
		result[6] = byte(blockHeader)
		result[7] = byte(blockHeader >> 8)
		result[8] = byte(blockHeader >> 16)
		copy(result[9:], data)
		return result
	}

	<-encodeSlots
	defer func() { encodeSlots <- struct{}{} }()

	// Compression is performed into a scratch buffer that is reused
	// across calls, so that the compressed representation can be copied
	// into a tight result buffer without allocating the scratch buffer
	// on every call.
	scratch := scratchPool.Get().(*[]byte)

	tmp := coderEncoder.EncodeAll(data, (*scratch)[:0])

	result := make([]byte, len(tmp))
	copy(result, tmp)
	scratchPool.Put(scratch)
	return result
}

// Decode decodes the provided data into a newly allocated byte slice of
// at most maximumDecodedSizeBytes.
//
// If the decoded representation is larger than maximumDecodedSizeBytes,
// an error is returned and no data is produced.
func Decode(data []byte, maximumDecodedSizeBytes int) ([]byte, error) {
	if maximumDecodedSizeBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "Maximum decoded size cannot be negative")
	}
	decoded, err := coderDecoder.DecodeAll(data, make([]byte, 0, maximumDecodedSizeBytes))
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to decode ZSTD data")
	}
	return decoded, nil
}
