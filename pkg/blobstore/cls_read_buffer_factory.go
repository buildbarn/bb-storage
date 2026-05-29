package blobstore

import (
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type clsReadBufferFactory struct{}

func (clsReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&remoteexecution.SplitBlobResponse{}, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (clsReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&remoteexecution.SplitBlobResponse{}, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f clsReadBufferFactory) NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromReaderAt(r), dataIntegrityCallback)
}

// CLSReadBufferFactory is capable of creating identifiers and buffers
// for objects stored in the Chunk List Storage (CLS).
var CLSReadBufferFactory ReadBufferFactory = clsReadBufferFactory{}
