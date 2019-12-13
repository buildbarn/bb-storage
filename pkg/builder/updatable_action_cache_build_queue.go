package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
)

type updatableActionCacheBuildQueue struct {
	base BuildQueue
}

// NewUpdatableActionCacheBuildQueue alters the response of
// GetCapabilities() to announce that this build queue allows direct
// writing to the Action Cache.
func NewUpdatableActionCacheBuildQueue(base BuildQueue) BuildQueue {
	return &updatableActionCacheBuildQueue{
		base: base,
	}
}

func (bq *updatableActionCacheBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	// Extract underlying capabilities.
	oldCapabilities, err := bq.base.GetCapabilities(ctx, in)
	if err != nil {
		return nil, err
	}

	// If CacheCapabilities are provided, alter them to announce
	// that the Action Cache permits updates.
	newCapabilities := proto.Clone(oldCapabilities).(*remoteexecution.ServerCapabilities)
	if cacheCapabilities := newCapabilities.CacheCapabilities; cacheCapabilities != nil {
		cacheCapabilities.ActionCacheUpdateCapabilities = &remoteexecution.ActionCacheUpdateCapabilities{
			UpdateEnabled: true,
		}
	}
	return newCapabilities, nil
}

func (bq *updatableActionCacheBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	return bq.base.Execute(in, out)
}

func (bq *updatableActionCacheBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	return bq.base.WaitExecution(in, out)
}
