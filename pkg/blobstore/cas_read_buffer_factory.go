package blobstore

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

type casReadBufferFactory struct{}

func (f casReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewCASBufferFromByteSlice(digest, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (f casReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewCASBufferFromReader(digest, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f casReadBufferFactory) NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromReaderAt(r), dataIntegrityCallback)
}

// CASReadBufferFactory is capable of creating buffers for objects
// stored in the Content Addressable Storage (CAS).
var CASReadBufferFactory ReadBufferFactory = casReadBufferFactory{}

// CASPutProto is a helper function for storing Protobuf messages in the
// Content Addressable Storage (CAS). It computes the digest of the
// message and stores it under that key. The digest is then returned, so
// that the object may be referenced.
func CASPutProto(ctx context.Context, blobAccess BlobAccess, message proto.Message, digestFunction digest.Function) (digest.Digest, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return digest.BadDigest, err
	}

	// Compute new digest of data.
	digestGenerator := digestFunction.NewGenerator()
	if _, err := digestGenerator.Write(data); err != nil {
		panic(err)
	}
	blobDigest := digestGenerator.Sum()

	if err := blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(data)); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}
