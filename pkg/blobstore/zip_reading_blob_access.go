package blobstore

import (
	"archive/zip"
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type zipReadingBlobAccess[T any] struct {
	capabilities.Provider
	decoder         coder.Decoder[T, []byte]
	digestKeyFormat digest.KeyFormat
	files           map[string]*zip.File
}

// NewZIPReadingBlobAccess creates a BlobAccess that is capable of
// reading objects from a ZIP archive. Depending on whether the
// containing files are compressed, files may either be randomly or
// sequentially accessible.
func NewZIPReadingBlobAccess[T any](capabilitiesProvider capabilities.Provider, decoder coder.Decoder[T, []byte], digestKeyFormat digest.KeyFormat, filesList []*zip.File) BlobAccess[T] {
	files := make(map[string]*zip.File, len(filesList))
	for _, file := range filesList {
		files[file.Name] = file
	}
	return &zipReadingBlobAccess[T]{
		Provider:        capabilitiesProvider,
		decoder:         decoder,
		digestKeyFormat: digestKeyFormat,
		files:           files,
	}
}

func (ba *zipReadingBlobAccess[T]) Get(ctx context.Context, blobDigest digest.Digest) (T, error) {
	var zero T
	key := blobDigest.GetKey(ba.digestKeyFormat)
	file, ok := ba.files[key]
	if !ok {
		return zero, status.Errorf(codes.NotFound, "File %#v not found in ZIP archive", key)
	}
	blobData := make([]byte, file.UncompressedSize64)
	r, err := file.Open()
	if err != nil {
		return zero, util.StatusWrapfWithCode(err, codes.Internal, "Failed to open file %#v in ZIP archive", key)
	}
	defer r.Close()
	if _, err := io.ReadFull(r, blobData); err != nil {
		return zero, util.StatusWrapf(err, "Failed to read entire file %#v", key)
	}
	return ba.decoder.Decode(blobData, blobDigest)
}

func (zipReadingBlobAccess[T]) Put(ctx context.Context, digest digest.Digest, value T) error {
	return status.Error(codes.InvalidArgument, "The ZIP reading storage backend does not permit writes")
}

func (ba *zipReadingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missing := digest.NewSetBuilder(0)
	for _, fileDigest := range digests.Items() {
		if _, ok := ba.files[fileDigest.GetKey(ba.digestKeyFormat)]; !ok {
			missing.Add(fileDigest)
		}
	}
	return missing.Build(), nil
}
