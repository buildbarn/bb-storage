package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nonExecutableBuildQueue struct {
}

// NewNonExecutableBuildQueue creates a build queue that is incapable of
// executing anything. It is merely needed to provide a functional
// implementation of GetCapabilities() for instances that provide remote
// caching without the execution.
func NewNonExecutableBuildQueue() BuildQueue {
	return &nonExecutableBuildQueue{}
}

func (bq *nonExecutableBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunction: []remoteexecution.DigestFunction{
				remoteexecution.DigestFunction_MD5,
				remoteexecution.DigestFunction_SHA1,
				remoteexecution.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			// CachePriorityCapabilities: Priorities not supported.
			// MaxBatchTotalSize: Not used by Bazel yet.
			SymlinkAbsolutePathStrategy: remoteexecution.CacheCapabilities_ALLOWED,
		},
		// TODO(edsch): DeprecatedApiVersion.
		LowApiVersion:  &semver.SemVer{Major: 2},
		HighApiVersion: &semver.SemVer{Major: 2},
	}, nil
}

func (bq *nonExecutableBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	return status.Errorf(codes.InvalidArgument, "This instance name cannot be used for remote execution; only remote caching")
}

func (bq *nonExecutableBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	return status.Errorf(codes.InvalidArgument, "This instance name cannot be used for remote execution; only remote caching")
}
