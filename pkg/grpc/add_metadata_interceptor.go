package grpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// NewAddMetadataUnaryClientInterceptor creates a gRPC request
// interceptor for unary calls that adds a set of specified pairs into
// the outgoing metadata headers. This may, for example, be used to perform
// authentication.
func NewAddMetadataUnaryClientInterceptor(pairs []string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req interface{}, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, pairs...), method, req, resp, cc, opts...)
	}
}

// NewAddMetadataStreamClientInterceptor creates a gRPC request
// interceptor for streaming calls that adds a set of specified pairs into
// the outgoing metadata headers. This may, for example, be used to perform
// authentication.
func NewAddMetadataStreamClientInterceptor(pairs []string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(metadata.AppendToOutgoingContext(ctx, pairs...), desc, cc, method, opts...)
	}
}
