package aliases

import (
	"crypto/cipher"
	"io"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/lossymap"
)

// This file contains aliases for some of the interfaces provided by the
// Go standard library. The only reason this file exists is to allow the
// gomock() Bazel rule to emit mocks for them, as that rule is only
// capable of emitting mocks for interfaces built through a
// go_library().

// AEAD is an alias of cipher.AEAD.
type AEAD = cipher.AEAD

// IntTreeDirectoryVisitor is a TreeDirectoryVisitor that takes integer
// arguments.
type IntTreeDirectoryVisitor = blobstore.TreeDirectoryVisitor[int]

// ReadCloser is an alias of io.ReadCloser.
type ReadCloser = io.ReadCloser

// RecordArray is declared for unit testing the hash map.
type RecordArray = lossymap.RecordArray[int, int, int]

// RecordKeyHasher is declared for unit testing the hash map.
type RecordKeyHasher = lossymap.RecordKeyHasher[int]

// ResponseWriter is an alias of http.ResponseWriter.
type ResponseWriter = http.ResponseWriter

// RoundTripper is an alias of http.RoundTripper.
type RoundTripper = http.RoundTripper

// ValueComparator is declared for unit testing the hash map.
type ValueComparator = lossymap.ValueComparator[int]

// Writer is an alias of io.Writer.
type Writer = io.Writer
