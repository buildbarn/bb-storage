package zstd_test

import (
	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func pseudoRandomData(tb testing.TB, size int) []byte {
	tb.Helper()
	data := make([]byte, size)
	_, err := rand.New(rand.NewSource(1)).Read(data)
	require.NoError(tb, err)
	return data
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	// Round tripping data is an inherent property of ZSTD. These
	// cases verify that the options chosen for the backing instances
	// do not break it for data with wildly different compression
	// characteristics.
	for name, data := range map[string][]byte{
		"Compressible":   bytes.Repeat([]byte("BuildBarn "), 10000),
		"Incompressible": pseudoRandomData(t, 1<<20),
		"Small":          []byte("Hello"),
		// Larger than the scratch buffer used by Encode(), so that
		// EncodeAll() needs to grow it.
		"Oversized": pseudoRandomData(t, 8<<20),
	} {
		t.Run(name, func(t *testing.T) {
			compressed := bb_zstd.Encode(data)
			require.Equal(t, len(compressed), cap(compressed), "Encode should return a tight byte slice")
			decoded, err := bb_zstd.Decode(compressed, len(data))
			require.NoError(t, err)
			require.Equal(t, data, decoded)
		})
	}
}

func TestEncodeEmpty(t *testing.T) {
	// Empty data is encoded as a frame that decodes to zero bytes.
	compressed := bb_zstd.Encode(nil)
	require.NotEmpty(t, compressed)

	decoded, err := bb_zstd.Decode(compressed, 0)
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func TestDecodeOfMalformedData(t *testing.T) {
	for name, data := range map[string][]byte{
		"Garbage":   []byte("garbage that is not ZSTD"),
		"Truncated": bb_zstd.Encode(bytes.Repeat([]byte("BuildBarn "), 1000))[:4],
	} {
		t.Run(name, func(t *testing.T) {
			_, err := bb_zstd.Decode(data, 10000)
			testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to decode ZSTD data: "), err)
		})
	}
}

func TestDecodeUnknownContentSizeLargerThanMaximum(t *testing.T) {
	// A streaming encoder does not know the size of the data upfront,
	// meaning that the frame header does not declare a content size.
	// Even so, decoding must not produce more output than the requested
	// maximum.
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf, zstd.WithWindowSize(1<<10))
	require.NoError(t, err)
	_, err = encoder.Write(bytes.Repeat([]byte("BuildBarn "), 10000))
	require.NoError(t, err)
	require.NoError(t, encoder.Close())

	_, err = bb_zstd.Decode(buf.Bytes(), 1)
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to decode ZSTD data: "), err)
}

func TestDecodeLargerThanMaximumDoesNotTruncate(t *testing.T) {
	// Data that decompresses to more than the requested maximum must
	// be rejected.
	known := bb_zstd.Encode(bytes.Repeat([]byte("BuildBarn "), 1000))
	_, err := bb_zstd.Decode(known, 1)
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to decode ZSTD data: "), err)

	// The same must hold when the frame does not declare its content
	// size, and when earlier frames of a concatenation would fit.
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf, zstd.WithWindowSize(1<<10))
	require.NoError(t, err)
	_, err = encoder.Write(bytes.Repeat([]byte("BuildBarn "), 1000))
	require.NoError(t, err)
	require.NoError(t, encoder.Close())
	_, err = bb_zstd.Decode(buf.Bytes(), 1)
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to decode ZSTD data: "), err)

	fitting := bb_zstd.Encode(bytes.Repeat([]byte("BuildBarn "), 50))
	_, err = bb_zstd.Decode(append(fitting, known...), 1)
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to decode ZSTD data: "), err)
}

func TestDecodeNegativeMaximumDecodedSize(t *testing.T) {
	compressed := bb_zstd.Encode([]byte("Hello"))
	_, err := bb_zstd.Decode(compressed, -1)
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Maximum decoded size cannot be negative"), err)
}

func TestEncodeAndDecodeSingleAllocation(t *testing.T) {
	// Incompressible data spanning several of the encoder's 128 KiB
	// blocks. Neither the number of blocks nor the compressibility of
	// the data affect the alloc behavior, as the result buffer is
	// allocated up front for everything the encoder may produce.
	data := pseudoRandomData(t, 512<<10)
	compressed := bb_zstd.Encode(data)
	decoded, err := bb_zstd.Decode(compressed, len(data))
	require.NoError(t, err)
	require.Equal(t, data, decoded)

	// The backing instances hold runtime.NumCPU() encoder/decoder
	// states with lazily allocated tables. Encode() and Decode()
	// round-robin through these states by means of a channel, so warm
	// up one state per iteration until all of them have reached their
	// steady size.
	for range runtime.NumCPU() {
		bb_zstd.Encode(data)
		_, err := bb_zstd.Decode(compressed, len(data))
		require.NoError(t, err)
	}

	runtime.GC()
	allocs := testing.AllocsPerRun(100, func() {
		bb_zstd.Encode(data)
	})
	require.Equal(t, 1.0, allocs, "Encode should only allocate the result buffer")

	runtime.GC()
	allocs = testing.AllocsPerRun(100, func() {
		decoded, err := bb_zstd.Decode(compressed, len(data))
		if err != nil {
			t.Errorf("Failed to decode: %s", err)
		}
		if !bytes.Equal(decoded, data) {
			t.Error("Decoded data does not match original")
		}
	})
	require.Equal(t, 1.0, allocs, "Decode should only allocate the result buffer")
}

func TestEncodeDecodeConcurrent(t *testing.T) {
	data := bytes.Repeat([]byte("BuildBarn "), 10000)
	compressed := bb_zstd.Encode(data)

	var wg sync.WaitGroup
	for range runtime.NumCPU() * 4 {
		wg.Go(func() {
			// Both functions take part in the same package-level
			// instances, so both are hammered.
			require.Equal(t, compressed, bb_zstd.Encode(data))
			decoded, err := bb_zstd.Decode(compressed, len(data))
			require.NoError(t, err)
			require.Equal(t, data, decoded)
		})
	}
	wg.Wait()
}

// FuzzEncodeDecode verifies the round trip property for arbitrary
// inputs, including the empty input.
func FuzzEncodeDecode(f *testing.F) {
	for i := range 4 {
		f.Add(pseudoRandomData(f, (256<<i)-1))
		f.Add(pseudoRandomData(f, 256<<i))
		f.Add(pseudoRandomData(f, (256<<i)+1))
	}
	f.Add([]byte("The quick brown fox jumps over the lazy dog."))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		compressed := bb_zstd.Encode(data)
		decoded, err := bb_zstd.Decode(compressed, len(data))
		require.NoError(t, err)
		require.Equal(t, data, decoded)
	})
}
