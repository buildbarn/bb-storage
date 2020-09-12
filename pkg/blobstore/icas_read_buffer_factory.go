package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
)

type icasReadBufferFactory struct{}

func (f icasReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&icas.Reference{}, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (f icasReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&icas.Reference{}, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f icasReadBufferFactory) NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromReaderAt(r), dataIntegrityCallback)
}

// ICASReadBufferFactory is capable of creating identifiers and buffers
// for objects stored in the Indirect Content Addressable Storage (ICAS).
var ICASReadBufferFactory ReadBufferFactory = icasReadBufferFactory{}
