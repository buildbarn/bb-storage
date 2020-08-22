package grpc

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/label"
)

// NewRequestMetadataFetchingUnaryServerInterceptor exports additional
// information about the RPC from REAPI request metadata into the
// current trace span.
func NewRequestMetadataFetchingUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		addMetadataToSpan(ctx)
		return handler(ctx, req)
	}
}

// NewRequestMetadataFetchingStreamServerInterceptor exports additional
// information about the RPC from REAPI request metadata into the
// current trace span.
func NewRequestMetadataFetchingStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		addMetadataToSpan(ss.Context())
		return handler(srv, ss)
	}
}

// TODO: test once there's a solution for tracking span attributes:
// https://github.com/census-instrumentation/opencensus-go/issues/293

func addMetadataToSpan(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return
	}

	rmds := md.Get("build.bazel.remote.execution.v2.requestmetadata-bin")
	if len(rmds) == 0 {
		return
	}

	var rmd remoteexecution.RequestMetadata

	if err := proto.Unmarshal([]byte(rmds[0]), &rmd); err != nil {
		return
	}

	span.SetAttributes(
		label.String("action_id", rmd.ActionId),
		label.String("tool_invocation_id", rmd.ToolInvocationId),
		label.String("correlated_invocations_id", rmd.CorrelatedInvocationsId),
	)

	if rmd.ToolDetails != nil {
		span.SetAttributes(
			label.String("tool_name", rmd.ToolDetails.ToolName),
			label.String("tool_version", rmd.ToolDetails.ToolVersion),
		)
	}
}
