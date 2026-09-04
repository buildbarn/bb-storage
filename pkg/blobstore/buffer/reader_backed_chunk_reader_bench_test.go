package buffer_test

import (
	"bytes"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
)

type readAtCloser struct{ *bytes.Reader }

func (readAtCloser) Close() error { return nil }

const benchChunkSize = 1 << 16

var benchBlobSizes = []struct {
	name     string
	blobSize int64
}{
	{"BlobSize=4KiB", 4 << 10},
	{"BlobSize=64KiB", 64 << 10},
	{"BlobSize=256KiB", 256 << 10},
	{"BlobSize=4MiB", 4 << 20},
	{"BlobSize=64MiB", 64 << 20},
}

// NewValidatedBufferFromReaderAt is the cheapest public entry point
// that resolves to newReaderBackedChunkReader without wrapping
// decorators.
func BenchmarkReaderBackedChunkReader(b *testing.B) {
	for _, c := range benchBlobSizes {
		b.Run(c.name, func(b *testing.B) {
			data := make([]byte, c.blobSize)
			for i := range data {
				data[i] = byte(i)
			}

			b.SetBytes(c.blobSize)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				buf := buffer.NewValidatedBufferFromReaderAt(readAtCloser{bytes.NewReader(data)}, c.blobSize)
				r := buf.ToChunkReader(0, benchChunkSize)
				for {
					if _, err := r.Read(); err != nil {
						break
					}
				}
				r.Close()
			}
		})
	}
}

func BenchmarkReaderBackedChunkReaderParallel(b *testing.B) {
	for _, c := range benchBlobSizes {
		b.Run(c.name, func(b *testing.B) {
			data := make([]byte, c.blobSize)
			for i := range data {
				data[i] = byte(i)
			}

			b.SetBytes(c.blobSize)
			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf := buffer.NewValidatedBufferFromReaderAt(readAtCloser{bytes.NewReader(data)}, c.blobSize)
					r := buf.ToChunkReader(0, benchChunkSize)
					for {
						if _, err := r.Read(); err != nil {
							break
						}
					}
					r.Close()
				}
			})
		})
	}
}
