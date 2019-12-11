package buffer

// ErrorHandler provides the possibility to hook into errors that occur
// on Buffer objects. There are three different use cases:
//
// - To capture errors that occur on buffers, as done by
//   MetricsBlobAccess.
// - To substitute or augment errors that occur on buffers, as done by
//   ExistencePreconditionBlobAccess.
// - To provide an alternative buffer where the desired content may be
//   obtained instead. This may be useful for storage backends capable
//   of replication.
//
// Every instance of ErrorHandler will have zero or more calls against
// OnError() performed against it, followed by a call to Done().
//
// ErrorHandler is only used by Buffer implementations in error
// scenarios where the transmission may continue if another buffer was
// provided, such as I/O errors. There are some errors conditions where
// ErrorHandler is not used, such as:
//
// - Passing a negative offset to ToChunkReader(). Switching to another
//   buffer would not yield any further progress.
// - Checksum mismatches on streams returned by ToChunkReader() and
//   ToReader(). The faulty data may have already been returned to the
//   consumer of the buffer through Read(), which cannot be undone by
//   switching to another buffer. Checksum mismatches returned by
//   ReadAt() and ToByteSlice() will be caught, as those can be retried
//   in their entirety.
type ErrorHandler interface {
	OnError(err error) (Buffer, error)
	Done()
}

// WithErrorHandler attaches an ErrorHandler to a Buffer. If the
// provided Buffer is already in a guaranteed success/failure state, the
// ErrorHandler may be applied immediately.
func WithErrorHandler(b Buffer, errorHandler ErrorHandler) Buffer {
	for {
		var retry bool
		b, retry = b.applyErrorHandler(errorHandler)
		if !retry {
			return b
		}
	}
}
