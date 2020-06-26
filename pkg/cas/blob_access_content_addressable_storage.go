package cas

import (
	"context"
	"io"
	"math"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/golang/protobuf/proto"
)

type blobAccessContentAddressableStorage struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewBlobAccessContentAddressableStorage creates a
// ContentAddressableStorage that reads and writes Content Addressable
// Storage (CAS) objects from a BlobAccess based store.
func NewBlobAccessContentAddressableStorage(blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) ContentAddressableStorage {
	return &blobAccessContentAddressableStorage{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (cas *blobAccessContentAddressableStorage) getMessage(ctx context.Context, digest digest.Digest, m proto.Message) (proto.Message, error) {
	return cas.blobAccess.Get(ctx, digest).ToProto(m, cas.maximumMessageSizeBytes)
}

func (cas *blobAccessContentAddressableStorage) GetAction(ctx context.Context, digest digest.Digest) (*remoteexecution.Action, error) {
	m, err := cas.getMessage(ctx, digest, &remoteexecution.Action{})
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Action), nil
}

func (cas *blobAccessContentAddressableStorage) GetUncachedActionResult(ctx context.Context, digest digest.Digest) (*cas_proto.UncachedActionResult, error) {
	m, err := cas.getMessage(ctx, digest, &cas_proto.UncachedActionResult{})
	if err != nil {
		return nil, err
	}
	return m.(*cas_proto.UncachedActionResult), nil
}

func (cas *blobAccessContentAddressableStorage) GetCommand(ctx context.Context, digest digest.Digest) (*remoteexecution.Command, error) {
	m, err := cas.getMessage(ctx, digest, &remoteexecution.Command{})
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Command), nil
}

func (cas *blobAccessContentAddressableStorage) GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error) {
	m, err := cas.getMessage(ctx, digest, &remoteexecution.Directory{})
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Directory), nil
}

func (cas *blobAccessContentAddressableStorage) GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name string, isExecutable bool) error {
	var mode os.FileMode = 0444
	if isExecutable {
		mode = 0555
	}

	w, err := directory.OpenAppend(name, filesystem.CreateExcl(mode))
	if err != nil {
		return err
	}
	defer w.Close()

	if err := cas.blobAccess.Get(ctx, digest).IntoWriter(w); err != nil {
		// Ensure no traces are left behind upon failure.
		directory.Remove(name)
		return err
	}
	time := filesystem.DeterministicFileModificationTimestamp
	if err := directory.Chtimes(name, time, time); err != nil {
		directory.Remove(name)
		return err
	}
	return nil
}

func (cas *blobAccessContentAddressableStorage) GetTree(ctx context.Context, digest digest.Digest) (*remoteexecution.Tree, error) {
	m, err := cas.getMessage(ctx, digest, &remoteexecution.Tree{})
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Tree), nil
}

func (cas *blobAccessContentAddressableStorage) putBlob(ctx context.Context, data []byte, parentDigest digest.Digest) (digest.Digest, error) {
	// Compute new digest of data.
	digestGenerator := parentDigest.NewGenerator()
	if _, err := digestGenerator.Write(data); err != nil {
		return digest.BadDigest, err
	}
	blobDigest := digestGenerator.Sum()

	if err := cas.blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(data)); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}

func (cas *blobAccessContentAddressableStorage) putMessage(ctx context.Context, message proto.Message, parentDigest digest.Digest) (digest.Digest, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return digest.BadDigest, err
	}
	return cas.putBlob(ctx, data, parentDigest)
}

func (cas *blobAccessContentAddressableStorage) PutFile(ctx context.Context, directory filesystem.Directory, name string, parentDigest digest.Digest) (digest.Digest, error) {
	file, err := directory.OpenRead(name)
	if err != nil {
		return digest.BadDigest, err
	}

	// Walk through the file to compute the digest.
	digestGenerator := parentDigest.NewGenerator()
	sizeBytes, err := io.Copy(digestGenerator, io.NewSectionReader(file, 0, math.MaxInt64))
	if err != nil {
		file.Close()
		return digest.BadDigest, err
	}
	blobDigest := digestGenerator.Sum()

	// Rewind and store it. Limit uploading to the size that was
	// used to compute the digest. This ensures uploads succeed,
	// even if more data gets appended in the meantime. This is not
	// uncommon, especially for stdout and stderr logs.
	if err := cas.blobAccess.Put(
		ctx,
		blobDigest,
		buffer.NewCASBufferFromReader(
			blobDigest,
			newSectionReadCloser(file, 0, sizeBytes),
			buffer.UserProvided)); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}

// newSectionReadCloser returns an io.ReadCloser that reads from r at a
// given offset, but stops with EOF after n bytes. This function is
// identical to io.NewSectionReader(), except that it provides an
// io.ReadCloser instead of an io.Reader.
func newSectionReadCloser(r filesystem.FileReader, off int64, n int64) io.ReadCloser {
	return &struct {
		io.SectionReader
		io.Closer
	}{
		SectionReader: *io.NewSectionReader(r, off, n),
		Closer:        r,
	}
}

func (cas *blobAccessContentAddressableStorage) PutLog(ctx context.Context, log []byte, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.putBlob(ctx, log, parentDigest)
}

func (cas *blobAccessContentAddressableStorage) PutTree(ctx context.Context, tree *remoteexecution.Tree, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.putMessage(ctx, tree, parentDigest)
}

func (cas *blobAccessContentAddressableStorage) PutUncachedActionResult(ctx context.Context, uncachedActionResult *cas_proto.UncachedActionResult, parentDigest digest.Digest) (digest.Digest, error) {
	return cas.putMessage(ctx, uncachedActionResult, parentDigest)
}
