package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	chunklist_pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/chunklist"
)

type clsReadBufferFactory struct{}

func (clsReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&chunklist_pb.ChunkList{}, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (clsReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&chunklist_pb.ChunkList{}, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f clsReadBufferFactory) NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromReaderAt(r), dataIntegrityCallback)
}

// CLSReadBufferFactory is capable of creating identifiers and buffers
// for objects stored in the Chunk List Storage (CLS).
var CLSReadBufferFactory ReadBufferFactory = clsReadBufferFactory{}
