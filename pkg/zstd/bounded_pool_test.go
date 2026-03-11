package zstd_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func newTestPool(maximumEncoders, maximumDecoders int64) bb_zstd.Pool {
	return bb_zstd.NewBoundedPool(
		maximumEncoders, maximumDecoders,
		[]zstd.EOption{zstd.WithEncoderConcurrency(1)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(1)},
	)
}

func TestBoundedPool_EncoderAcquireRelease(t *testing.T) {
	pool := newTestPool(2, 2)

	var buf bytes.Buffer
	enc, err := pool.NewEncoder(context.Background(), &buf)
	require.NoError(t, err)
	require.NotNil(t, enc)

	_, err = enc.Write([]byte("hello world"))
	require.NoError(t, err)
	require.NoError(t, enc.Close())

	// Verify we can acquire again (reuses the same encoder).
	var buf2 bytes.Buffer
	enc2, err := pool.NewEncoder(context.Background(), &buf2)
	require.NoError(t, err)
	require.NotNil(t, enc2)
	require.NoError(t, enc2.Close())
}

func TestBoundedPool_DecoderAcquireRelease(t *testing.T) {
	pool := newTestPool(2, 2)

	var compressed bytes.Buffer
	enc, err := pool.NewEncoder(context.Background(), &compressed)
	require.NoError(t, err)

	testData := []byte("hello world, this is test data for compression")
	_, err = enc.Write(testData)
	require.NoError(t, err)
	require.NoError(t, enc.Close())

	dec, err := pool.NewDecoder(context.Background(), bytes.NewReader(compressed.Bytes()))
	require.NoError(t, err)
	require.NotNil(t, dec)

	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)

	dec.Close()
}

func TestBoundedPool_ConcurrencyLimit(t *testing.T) {
	pool := newTestPool(2, 2)

	var buf1, buf2 bytes.Buffer
	enc1, err := pool.NewEncoder(context.Background(), &buf1)
	require.NoError(t, err)
	enc2, err := pool.NewEncoder(context.Background(), &buf2)
	require.NoError(t, err)

	// Third acquire should block and time out.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var buf3 bytes.Buffer
	_, err = pool.NewEncoder(ctx, &buf3)
	require.Error(t, err)

	// Release one encoder via Close(), then acquire should succeed.
	require.NoError(t, enc1.Close())

	enc3, err := pool.NewEncoder(context.Background(), &buf3)
	require.NoError(t, err)
	require.NotNil(t, enc3)

	require.NoError(t, enc2.Close())
	require.NoError(t, enc3.Close())
}

func TestBoundedPool_ConcurrentAccess(t *testing.T) {
	pool := newTestPool(4, 4)

	testData := []byte("concurrent test data that needs to be compressed and decompressed")

	var compressed bytes.Buffer
	enc, _ := pool.NewEncoder(context.Background(), &compressed)
	enc.Write(testData)
	enc.Close()
	compressedBytes := compressed.Bytes()

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var buf bytes.Buffer
			enc, err := pool.NewEncoder(context.Background(), &buf)
			if err != nil {
				errs <- err
				return
			}
			enc.Write(testData)
			enc.Close()

			dec, err := pool.NewDecoder(context.Background(), bytes.NewReader(compressedBytes))
			if err != nil {
				errs <- err
				return
			}
			result, err := io.ReadAll(dec)
			dec.Close()
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(result, testData) {
				errs <- fmt.Errorf("decompressed data mismatch: got %d bytes, want %d bytes", len(result), len(testData))
				return
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent operation failed: %v", err)
	}
}

func TestBoundedPool_ContextCancellation(t *testing.T) {
	pool := newTestPool(1, 1)

	var buf bytes.Buffer
	enc, _ := pool.NewEncoder(context.Background(), &buf)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf2 bytes.Buffer
	_, err := pool.NewEncoder(ctx, &buf2)
	require.Error(t, err)

	enc.Close()
}

func TestBoundedPool_DoubleClose(t *testing.T) {
	pool := newTestPool(2, 2)

	// Double close should not panic or release semaphore twice.
	var buf bytes.Buffer
	enc, err := pool.NewEncoder(context.Background(), &buf)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	require.NoError(t, enc.Close())
}

func BenchmarkBoundedPool_AcquireRelease(b *testing.B) {
	pool := newTestPool(16, 16)
	testData := bytes.Repeat([]byte("benchmark data "), 1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var buf bytes.Buffer
			enc, _ := pool.NewEncoder(context.Background(), &buf)
			enc.Write(testData)
			enc.Close()
		}
	})
}

func BenchmarkZstdNoPool(b *testing.B) {
	testData := bytes.Repeat([]byte("benchmark data "), 1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var buf bytes.Buffer
			enc, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
			enc.Write(testData)
			enc.Close()
		}
	})
}
