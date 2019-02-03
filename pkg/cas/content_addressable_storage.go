package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// ContentAddressableStorage provides typed access to a Bazel Content
// Addressable Storage (CAS).
type ContentAddressableStorage interface {
	GetAction(ctx context.Context, digest *util.Digest) (*remoteexecution.Action, error)
	GetCommand(ctx context.Context, digest *util.Digest) (*remoteexecution.Command, error)
	GetDirectory(ctx context.Context, digest *util.Digest) (*remoteexecution.Directory, error)
	GetFile(ctx context.Context, digest *util.Digest, directory filesystem.Directory, name string, isExecutable bool) error
	GetTree(ctx context.Context, digest *util.Digest) (*remoteexecution.Tree, error)
	GetUncachedActionResult(ctx context.Context, digest *util.Digest) (*cas_proto.UncachedActionResult, error)

	PutFile(ctx context.Context, directory filesystem.Directory, name string, parentDigest *util.Digest) (*util.Digest, error)
	PutLog(ctx context.Context, log []byte, parentDigest *util.Digest) (*util.Digest, error)
	PutTree(ctx context.Context, tree *remoteexecution.Tree, parentDigest *util.Digest) (*util.Digest, error)
	PutUncachedActionResult(ctx context.Context, uncachedActionResult *cas_proto.UncachedActionResult, parentDigest *util.Digest) (*util.Digest, error)
}
