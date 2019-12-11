package buffer

import (
	"io"
)

type errorHandlingChunkReader struct {
	r                     ChunkReader
	errorHandler          ErrorHandler
	off                   int64
	maximumChunkSizeBytes int
}

// newErrorHandlingChunkReader returns a ChunkReader that forwards calls
// to a reader obtained from a Buffer. Upon I/O failure, it calls into
// an ErrorHandler to request a new Buffer to continue the transfer.
func newErrorHandlingChunkReader(b Buffer, errorHandler ErrorHandler, off int64, maximumChunkSizeBytes int) ChunkReader {
	return &errorHandlingChunkReader{
		r:                     b.toUnvalidatedChunkReader(off, maximumChunkSizeBytes),
		errorHandler:          errorHandler,
		off:                   off,
		maximumChunkSizeBytes: maximumChunkSizeBytes,
	}
}

func (r *errorHandlingChunkReader) Read() ([]byte, error) {
	for {
		chunk, originalErr := r.r.Read()
		if originalErr == nil {
			r.off += int64(len(chunk))
			return chunk, nil
		} else if originalErr == io.EOF {
			return nil, io.EOF
		}
		b, translatedErr := r.errorHandler.OnError(originalErr)
		if translatedErr != nil {
			return nil, translatedErr
		}
		r.r.Close()
		r.r = b.toUnvalidatedChunkReader(r.off, r.maximumChunkSizeBytes)
	}
}

func (r *errorHandlingChunkReader) Close() {
	r.errorHandler.Done()
	r.r.Close()
}
