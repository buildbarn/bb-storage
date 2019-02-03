package cas

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/proto"
)

type blobAccessContentAddressableStorage struct {
	blobAccess blobstore.BlobAccess
}

// NewBlobAccessContentAddressableStorage creates a
// ContentAddressableStorage that reads and writes Content Adressable
// Storage (CAS) objects from a BlobAccess based store.
func NewBlobAccessContentAddressableStorage(blobAccess blobstore.BlobAccess) ContentAddressableStorage {
	return &blobAccessContentAddressableStorage{
		blobAccess: blobAccess,
	}
}

func (cas *blobAccessContentAddressableStorage) getMessage(ctx context.Context, digest *util.Digest, message proto.Message) error {
	// TODO(edsch): Reject fetching overly large blobs.
	_, r, err := cas.blobAccess.Get(ctx, digest)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, message)
}

func (cas *blobAccessContentAddressableStorage) GetAction(ctx context.Context, digest *util.Digest) (*remoteexecution.Action, error) {
	var action remoteexecution.Action
	if err := cas.getMessage(ctx, digest, &action); err != nil {
		return nil, err
	}
	return &action, nil
}

func (cas *blobAccessContentAddressableStorage) GetUncachedActionResult(ctx context.Context, digest *util.Digest) (*cas_proto.UncachedActionResult, error) {
	var uncachedActionResult cas_proto.UncachedActionResult
	if err := cas.getMessage(ctx, digest, &uncachedActionResult); err != nil {
		return nil, err
	}
	return &uncachedActionResult, nil
}

func (cas *blobAccessContentAddressableStorage) GetCommand(ctx context.Context, digest *util.Digest) (*remoteexecution.Command, error) {
	var command remoteexecution.Command
	if err := cas.getMessage(ctx, digest, &command); err != nil {
		return nil, err
	}
	return &command, nil
}

func (cas *blobAccessContentAddressableStorage) GetDirectory(ctx context.Context, digest *util.Digest) (*remoteexecution.Directory, error) {
	var directory remoteexecution.Directory
	if err := cas.getMessage(ctx, digest, &directory); err != nil {
		return nil, err
	}
	return &directory, nil
}

func (cas *blobAccessContentAddressableStorage) GetFile(ctx context.Context, digest *util.Digest, directory filesystem.Directory, name string, isExecutable bool) error {
	var mode os.FileMode = 0444
	if isExecutable {
		mode = 0555
	}
	w, err := directory.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer w.Close()

	_, r, err := cas.blobAccess.Get(ctx, digest)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	r.Close()

	// Ensure no traces are left behind upon failure.
	if err != nil {
		directory.Remove(name)
	}
	return err
}

func (cas *blobAccessContentAddressableStorage) GetTree(ctx context.Context, digest *util.Digest) (*remoteexecution.Tree, error) {
	var tree remoteexecution.Tree
	if err := cas.getMessage(ctx, digest, &tree); err != nil {
		return nil, err
	}
	return &tree, nil
}

func (cas *blobAccessContentAddressableStorage) putBlob(ctx context.Context, data []byte, parentDigest *util.Digest) (*util.Digest, error) {
	// Compute new digest of data.
	digestGenerator := parentDigest.NewDigestGenerator()
	if _, err := digestGenerator.Write(data); err != nil {
		return nil, err
	}
	digest := digestGenerator.Sum()

	if err := cas.blobAccess.Put(ctx, digest, digest.GetSizeBytes(), ioutil.NopCloser(bytes.NewBuffer(data))); err != nil {
		return nil, err
	}
	return digest, nil
}

func (cas *blobAccessContentAddressableStorage) putMessage(ctx context.Context, message proto.Message, parentDigest *util.Digest) (*util.Digest, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return cas.putBlob(ctx, data, parentDigest)
}

func (cas *blobAccessContentAddressableStorage) PutFile(ctx context.Context, directory filesystem.Directory, name string, parentDigest *util.Digest) (*util.Digest, error) {
	file, err := directory.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	// Walk through the file to compute the digest.
	digestGenerator := parentDigest.NewDigestGenerator()
	if _, err = io.Copy(digestGenerator, file); err != nil {
		file.Close()
		return nil, err
	}
	digest := digestGenerator.Sum()

	// Rewind and store it.
	if _, err := file.Seek(0, 0); err != nil {
		file.Close()
		return nil, err
	}
	if err := cas.blobAccess.Put(ctx, digest, digest.GetSizeBytes(), file); err != nil {
		return nil, err
	}
	return digest, nil
}

func (cas *blobAccessContentAddressableStorage) PutLog(ctx context.Context, log []byte, parentDigest *util.Digest) (*util.Digest, error) {
	return cas.putBlob(ctx, log, parentDigest)
}

func (cas *blobAccessContentAddressableStorage) PutTree(ctx context.Context, tree *remoteexecution.Tree, parentDigest *util.Digest) (*util.Digest, error) {
	return cas.putMessage(ctx, tree, parentDigest)
}

func (cas *blobAccessContentAddressableStorage) PutUncachedActionResult(ctx context.Context, uncachedActionResult *cas_proto.UncachedActionResult, parentDigest *util.Digest) (*util.Digest, error) {
	return cas.putMessage(ctx, uncachedActionResult, parentDigest)
}
