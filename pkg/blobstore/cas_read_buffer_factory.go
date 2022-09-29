package blobstore

import (
	"context"
	"io"
	"math"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/encoding/protowire"
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
	bDigest, bPut := buffer.NewProtoBufferFromProto(message, buffer.UserProvided).CloneCopy(math.MaxInt)

	// Compute new digest of data.
	digestGenerator := digestFunction.NewGenerator()
	if err := bDigest.IntoWriter(digestGenerator); err != nil {
		bPut.Discard()
		return digest.BadDigest, err
	}
	blobDigest := digestGenerator.Sum()

	if err := blobAccess.Put(ctx, blobDigest, bPut); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}

// The Protobuf field numbers of the REv2 Tree's "root" and "children"
// fields. These are used in combination with util.VisitProtoBytesFields()
// to be able to process REv2 Tree objects in a streaming manner.
const (
	TreeRootFieldNumber     protowire.Number = 1
	TreeChildrenFieldNumber protowire.Number = 2
)
