package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// MustStreamMethodFromContext returns the service and method name for the ongoing gRPC stream.
// It will panic if the given context has no grpc.ServerTransportStream associated with it
// (which implies it is not an RPC invocation context).
func MustStreamMethodFromContext(ctx context.Context) string {
	transportStream := grpc.ServerTransportStreamFromContext(ctx)
	if transportStream == nil {
		panic("No grpc.ServerTransportStream in context")
	}
	return transportStream.Method()
}
