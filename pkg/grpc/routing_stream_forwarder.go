package grpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RoutingStreamForwarder forwards gRPC streams to different backends depending
// on the method being invoked.
type RoutingStreamForwarder struct {
	// RouteTable maps to the grpc.StreamHandler to be called. The key is the
	// combined gRPC service and method name.
	RouteTable map[string]grpc.StreamHandler
}

// NewRoutingStreamForwarder creates a RoutingStreamForwarder which routes gRPC
// streams based on the invoked gRPC method name.
func NewRoutingStreamForwarder() *RoutingStreamForwarder {
	return &RoutingStreamForwarder{
		RouteTable: make(map[string]grpc.StreamHandler),
	}
}

// HandleStream is the implementation of the grpc.StreamHandler interface to
// process a gRPC stream, forwarding it according to the RouteTable.
func (s *RoutingStreamForwarder) HandleStream(srv any, stream grpc.ServerStream) error {
	method := MustStreamMethodFromContext(stream.Context())
	if streamHandler, ok := s.RouteTable[method]; ok {
		return streamHandler(srv, stream)
	}
	return status.Errorf(codes.Unimplemented, "No route for method %v", method)
}
