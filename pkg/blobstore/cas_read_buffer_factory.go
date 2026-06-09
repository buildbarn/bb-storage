package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/encoding/protowire"
)

type casReadBufferFactory struct{}

func (casReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewCASBufferFromByteSlice(digest, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (casReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewCASBufferFromReader(digest, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f casReadBufferFactory) NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromReaderAt(r), dataIntegrityCallback)
}

// CASReadBufferFactory is capable of creating buffers for objects
// stored in the Content Addressable Storage (CAS).
var CASReadBufferFactory ReadBufferFactory = casReadBufferFactory{}

// The Protobuf field numbers of the REv2 Tree's "root" and "children"
// fields. These are used in combination with util.VisitProtoBytesFields()
// to be able to process REv2 Tree objects in a streaming manner.
const (
	TreeRootFieldNumber     protowire.Number = 1
	TreeChildrenFieldNumber protowire.Number = 2
)
