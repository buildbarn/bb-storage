package grpc

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"go.opencensus.io/trace"
)

// NewRequestMetadataFetchingStatsHandler is meant to wrap ocgrpc.ServerHandler,
// and exports additional information about the RPC from REAPI request metadata
func NewRequestMetadataFetchingStatsHandler(base stats.Handler) stats.Handler {
	return requestMetadataFetchingStatsHandler{base}
}

type requestMetadataFetchingStatsHandler struct {
	stats.Handler
}

// TODO: test once there's a solution for tracking span attributes:
// https://github.com/census-instrumentation/opencensus-go/issues/293

func (rmfsh requestMetadataFetchingStatsHandler) TagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	ctx = rmfsh.Handler.TagRPC(ctx, rti)

	span := trace.FromContext(ctx)
	if span == nil {
		return ctx
	}
	if !span.IsRecordingEvents() {
		return ctx
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	rmds := md.Get("build.bazel.remote.execution.v2.requestmetadata-bin")
	if len(rmds) == 0 {
		return ctx
	}

	var rmd remoteexecution.RequestMetadata

	if err := proto.Unmarshal([]byte(rmds[0]), &rmd); err != nil {
		return ctx
	}

	span.AddAttributes(
		trace.StringAttribute("action_id", rmd.ActionId),
		trace.StringAttribute("tool_invocation_id", rmd.ToolInvocationId),
		trace.StringAttribute("correlated_invocations_id", rmd.CorrelatedInvocationsId),
	)

	if rmd.ToolDetails != nil {
		span.AddAttributes(
			trace.StringAttribute("tool_name", rmd.ToolDetails.ToolName),
			trace.StringAttribute("tool_version", rmd.ToolDetails.ToolVersion),
		)
	}
	return ctx
}
