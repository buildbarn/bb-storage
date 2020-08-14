package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"go.opencensus.io/trace"
)

type tracingBuildQueue struct {
	base BuildQueue
}

// NewTracingBuildQueue injects BuildQueue annotations into trace spans
func NewTracingBuildQueue(base BuildQueue) BuildQueue {
	return &tracingBuildQueue{
		base: base,
	}
}

func (bq *tracingBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	trace.FromContext(ctx).Annotate([]trace.Attribute{
		trace.StringAttribute("instance", in.InstanceName),
	}, "BuildQueue.GetCapabilities")
	return bq.base.GetCapabilities(ctx, in)
}

func (bq *tracingBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	trace.FromContext(out.Context()).Annotate([]trace.Attribute{
		trace.StringAttribute("instance", in.InstanceName),
		trace.StringAttribute("digest", in.ActionDigest.Hash),
	}, "BuildQueue.Execute")
	return bq.base.Execute(in, out)
}

func (bq *tracingBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	trace.FromContext(out.Context()).Annotate([]trace.Attribute{
		trace.StringAttribute("name", in.Name),
	}, "BuildQueue.WaitExecution")
	return bq.base.WaitExecution(in, out)
}
