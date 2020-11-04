package grpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func forwardMetadataHeaders(ctx context.Context, headers []string) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	// Turn all matching headers into a flat sequence of key-value
	// pairs, as required by metadata.AppendToOutgoingContext().
	var headerValues MetadataHeaderValues
	for _, key := range headers {
		headerValues.Add(key, md.Get(key))
	}

	if len(headerValues) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, headerValues...)
}

// NewMetadataForwardingUnaryClientInterceptor creates a gRPC request
// interceptor for unary calls that extracts a set of incoming metadata
// headers from the calling context and copies them into the outgoing
// metadata headers. This may, for example, be used to perform
// credential forwarding.
func NewMetadataForwardingUnaryClientInterceptor(headers []string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(forwardMetadataHeaders(ctx, headers), method, req, resp, cc, opts...)
	}
}

// NewMetadataForwardingStreamClientInterceptor creates a gRPC request
// interceptor for streaming calls that extracts a set of incoming
// metadata headers from the calling context and copies them into the
// outgoing metadata headers. This may, for example, be used to perform
// credential forwarding.
func NewMetadataForwardingStreamClientInterceptor(headers []string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(forwardMetadataHeaders(ctx, headers), desc, cc, method, opts...)
	}
}
