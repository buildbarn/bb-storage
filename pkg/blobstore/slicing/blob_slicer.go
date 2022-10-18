package slicing

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BlobSlice describes a region of a parent object that should be
// requestable through BlobAccess.GetFromComposite().
//
// In the case of an REv2 Tree object, a BlobSlice may refer to the byte
// range within the Tree at which a single child Directory object is
// situated.
type BlobSlice struct {
	Digest      digest.Digest
	OffsetBytes int64
	SizeBytes   int64
}

// BlobSlicer is called into by BlobAccess.GetFromComposite() if the
// requested child object is not present in the data store. BlobSlicer
// is responsible for inspecting the contents of the parent object and
// slicing it up in smaller parts, so that the original read request may
// be satisfied.
//
// This type can, for example, be used to slice an REv2 Tree object into
// Directory objects. Once the Tree objec thas been sliced, the
// Directory objects can be read individually, thereby permitting fast
// random access.
type BlobSlicer interface {
	Slice(b buffer.Buffer, childDigest digest.Digest) (buffer.Buffer, []BlobSlice)
}
