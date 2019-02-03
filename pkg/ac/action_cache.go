package ac

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// ActionCache provides typed access to a Bazel Action Cache (AC).
type ActionCache interface {
	GetActionResult(ctx context.Context, digest *util.Digest) (*remoteexecution.ActionResult, error)
	PutActionResult(ctx context.Context, digest *util.Digest, result *remoteexecution.ActionResult) error
}
