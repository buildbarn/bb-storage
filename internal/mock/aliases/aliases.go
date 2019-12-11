package aliases

import (
	"io"
)

// This file contains aliases for some of the interfaces provided by the
// Go standard library. The only reason this file exists is to allow the
// gomock() Bazel rule to emit mocks for them, as that rule is only
// capable of emitting mocks for interfaces built through a
// go_library().

// ReadCloser is an alias of io.ReadCloser.
type ReadCloser = io.ReadCloser

// Writer is an alias of io.Writer.
type Writer = io.Writer
