package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
)

type fsacReadBufferFactory struct{}

func (f fsacReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&fsac.FileSystemAccessProfile{}, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (f fsacReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&fsac.FileSystemAccessProfile{}, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f fsacReadBufferFactory) NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromReaderAt(r), dataIntegrityCallback)
}

// FSACReadBufferFactory is capable of creating identifiers and buffers
// for objects stored in the File System Access Cache (FSAC).
var FSACReadBufferFactory ReadBufferFactory = fsacReadBufferFactory{}
