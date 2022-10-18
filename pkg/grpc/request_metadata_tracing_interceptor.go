package grpc

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// RequestMetadataTracingUnaryInterceptor is a gRPC unary server
// interceptor that adds additional information about the RPC from the
// REv2 RequestMetadata into the current trace span.
func RequestMetadataTracingUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	addRequestMetadataToSpan(ctx)
	return handler(ctx, req)
}

// RequestMetadataTracingStreamInterceptor is a gRPC streaming server
// interceptor that adds additional information about the RPC from the
// REv2 RequestMetadata into the current trace span.
func RequestMetadataTracingStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	addRequestMetadataToSpan(ss.Context())
	return handler(srv, ss)
}

func addRequestMetadataToSpan(ctx context.Context) {
	// Skip all of the code below when this request isn't being traced.
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	// Extract the RequestMetadata from gRPC metadata.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return
	}
	rmds := md.Get("build.bazel.remote.execution.v2.requestmetadata-bin")
	if len(rmds) == 0 {
		return
	}
	var requestMetadata remoteexecution.RequestMetadata
	if err := proto.Unmarshal([]byte(rmds[0]), &requestMetadata); err != nil {
		return
	}

	attributes := append(
		make([]attribute.KeyValue, 0, 8),
		attribute.String("request_metadata.action_id", requestMetadata.ActionId),
		attribute.String("request_metadata.action_mnemonic", requestMetadata.ActionMnemonic),
		attribute.String("request_metadata.configuration_id", requestMetadata.ConfigurationId),
		attribute.String("request_metadata.correlated_invocations_id", requestMetadata.CorrelatedInvocationsId),
		attribute.String("request_metadata.target_id", requestMetadata.TargetId),
		attribute.String("request_metadata.tool_invocation_id", requestMetadata.ToolInvocationId))
	if toolDetails := requestMetadata.ToolDetails; toolDetails != nil {
		attributes = append(attributes,
			attribute.String("request_metadata.tool_details.tool_name", toolDetails.ToolName),
			attribute.String("request_metadata.tool_details.tool_version", toolDetails.ToolVersion))
	}
	span.SetAttributes(attributes...)
}
