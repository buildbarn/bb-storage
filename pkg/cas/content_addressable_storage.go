package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	cas_proto "github.com/buildbarn/bb-storage/pkg/proto/cas"
)

// ContentAddressableStorage provides typed access to a Bazel Content
// Addressable Storage (CAS).
//
// TODO: Now that we have Buffer.ToProto(), this interface has become a
// lot less useful. Should we remove this, just like how we removed
// ActionCache when we added Buffer.ToActionResult()?
type ContentAddressableStorage interface {
	GetAction(ctx context.Context, digest digest.Digest) (*remoteexecution.Action, error)
	GetCommand(ctx context.Context, digest digest.Digest) (*remoteexecution.Command, error)
	GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error)
	GetFile(ctx context.Context, digest digest.Digest, directory filesystem.Directory, name string, isExecutable bool) error
	GetTree(ctx context.Context, digest digest.Digest) (*remoteexecution.Tree, error)
	GetUncachedActionResult(ctx context.Context, digest digest.Digest) (*cas_proto.UncachedActionResult, error)

	PutFile(ctx context.Context, directory filesystem.Directory, name string, parentDigest digest.Digest) (digest.Digest, error)
	PutLog(ctx context.Context, log []byte, parentDigest digest.Digest) (digest.Digest, error)
	PutTree(ctx context.Context, tree *remoteexecution.Tree, parentDigest digest.Digest) (digest.Digest, error)
	PutUncachedActionResult(ctx context.Context, uncachedActionResult *cas_proto.UncachedActionResult, parentDigest digest.Digest) (digest.Digest, error)
}
