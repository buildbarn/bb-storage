package blobstore

import (
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

type acReadBufferFactory struct{}

func (f acReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&remoteexecution.ActionResult{}, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (f acReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&remoteexecution.ActionResult{}, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f acReadBufferFactory) NewBufferFromFileReader(digest digest.Digest, r filesystem.FileReader, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromFileReader(r), dataIntegrityCallback)
}

// ACReadBufferFactory is capable of buffers for objects stored in the
// Action Cache (AC).
var ACReadBufferFactory ReadBufferFactory = acReadBufferFactory{}
