package buffer

import (
	"io"
)

type errorHandlingReader struct {
	r            io.ReadCloser
	errorHandler ErrorHandler
	off          int64
}

// newErrorHandlingReader returns an io.ReadCloser that forwards calls
// to a reader obtained from a Buffer. Upon I/O failure, it calls into
// an ErrorHandler to request a new Buffer to continue the transfer.
func newErrorHandlingReader(b Buffer, errorHandler ErrorHandler, off int64) io.ReadCloser {
	return &errorHandlingReader{
		r:            b.toUnvalidatedReader(off),
		errorHandler: errorHandler,
		off:          off,
	}
}

func (r *errorHandlingReader) Read(p []byte) (int, error) {
	n, originalErr := r.r.Read(p)
	r.off += int64(n)
	if originalErr == nil || originalErr == io.EOF {
		return n, originalErr
	}
	b, translatedErr := r.errorHandler.OnError(originalErr)
	if translatedErr != nil {
		return n, translatedErr
	}
	r.r.Close()
	r.r = b.toUnvalidatedReader(r.off)
	return n, nil
}

func (r *errorHandlingReader) Close() error {
	r.errorHandler.Done()
	return r.r.Close()
}
