package blobstore

import (
	"archive/zip"
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type zipReadingBlobAccess struct {
	capabilities.Provider
	readBufferFactory ReadBufferFactory
	digestKeyFormat   digest.KeyFormat
	files             map[string]*zip.File
}

// NewZIPReadingBlobAccess creates a BlobAccess that is capable of
// reading objects from a ZIP archive. Depending on whether the
// containing files are compressed, files may either be randomly or
// sequentially accessible.
func NewZIPReadingBlobAccess(capabilitiesProvider capabilities.Provider, readBufferFactory ReadBufferFactory, digestKeyFormat digest.KeyFormat, filesList []*zip.File) BlobAccess {
	files := make(map[string]*zip.File, len(filesList))
	for _, file := range filesList {
		files[file.Name] = file
	}
	return &zipReadingBlobAccess{
		Provider:          capabilitiesProvider,
		readBufferFactory: readBufferFactory,
		digestKeyFormat:   digestKeyFormat,
		files:             files,
	}
}

func (ba *zipReadingBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	key := blobDigest.GetKey(ba.digestKeyFormat)
	file, ok := ba.files[key]
	if !ok {
		return buffer.NewBufferFromError(status.Errorf(codes.NotFound, "File %#v not found in ZIP archive", key))
	}

	if file.Method == zip.Store && file.CompressedSize64 == file.UncompressedSize64 {
		// File is not compressed. Open it in raw mode,
		// so that we can perform random access.
		r, err := file.OpenRaw()
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapfWithCode(err, codes.Internal, "Failed to open file %#v in ZIP archive", key))
		}
		return ba.readBufferFactory.NewBufferFromReaderAt(
			blobDigest,
			nopAtCloser{ReaderAt: r.(io.ReaderAt)},
			int64(file.UncompressedSize64),
			buffer.Irreparable(blobDigest))
	}

	// File is compressed. Open it for sequential access.
	r, err := file.Open()
	if err != nil {
		return buffer.NewBufferFromError(util.StatusWrapfWithCode(err, codes.Internal, "Failed to open file %#v in ZIP archive", key))
	}
	return ba.readBufferFactory.NewBufferFromReader(
		blobDigest,
		io.NopCloser(r),
		buffer.Irreparable(blobDigest))
}

func (ba *zipReadingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	// TODO: We can provide a better implementation that stores the
	// resulting slices.
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *zipReadingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	b.Discard()
	return status.Error(codes.InvalidArgument, "The ZIP reading storage backend does not permit writes")
}

func (ba *zipReadingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missing := digest.NewSetBuilder()
	for _, fileDigest := range digests.Items() {
		if _, ok := ba.files[fileDigest.GetKey(ba.digestKeyFormat)]; !ok {
			missing.Add(fileDigest)
		}
	}
	return missing.Build(), nil
}

type nopAtCloser struct {
	io.ReaderAt
}

func (f nopAtCloser) Close() error {
	return nil
}
