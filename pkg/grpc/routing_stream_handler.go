package grpc

import (
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewRoutingStreamHandler creates a RoutingStreamForwarder which routes gRPC
// streams based on the invoked gRPC method name. The keys in the routeTable map
// are gRPC service names, for example:
//
// build.bazel.remote.execution.v2.Execution
// com.google.devtools.build.v1.PublishBuildEvent
func NewRoutingStreamHandler(routeTable map[string]grpc.StreamHandler) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		// All gRPC invocations has a grpc.ServerTransportStream context.
		orgServiceMethod := grpc.ServerTransportStreamFromContext(stream.Context()).Method()
		// Service and method name parsing based on grpc.Server.handleStream().
		serviceMethod := strings.TrimPrefix(orgServiceMethod, "/")
		endIdx := strings.LastIndex(serviceMethod, "/")
		if endIdx == -1 {
			return status.Errorf(codes.InvalidArgument, "Malformed method name %v", orgServiceMethod)
		}
		service := serviceMethod[:endIdx]

		if streamHandler, ok := routeTable[service]; ok {
			return streamHandler(srv, stream)
		}
		return status.Errorf(codes.Unimplemented, "No route for service %v", service)
	}
}
