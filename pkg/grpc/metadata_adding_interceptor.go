package grpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// NewMetadataAddingUnaryClientInterceptor creates a gRPC request
// interceptor for unary calls that adds a set of specified header
// values into the outgoing metadata headers. This may, for example, be
// used to perform authentication.
func NewMetadataAddingUnaryClientInterceptor(headerValues MetadataHeaderValues) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req interface{}, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, headerValues...), method, req, resp, cc, opts...)
	}
}

// NewMetadataAddingStreamClientInterceptor creates a gRPC request
// interceptor for streaming calls that adds a set of specified header
// values into the outgoing metadata headers. This may, for example, be
// used to perform authentication.
func NewMetadataAddingStreamClientInterceptor(headerValues MetadataHeaderValues) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(metadata.AppendToOutgoingContext(ctx, headerValues...), desc, cc, method, opts...)
	}
}
