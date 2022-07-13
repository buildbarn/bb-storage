package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MetadataExtractor is a function which extracts metadata values from a
// context.
type MetadataExtractor func(context.Context) (MetadataHeaderValues, error)

// NewMetadataExtractingAndForwardingUnaryClientInterceptor creates a gRPC
// request interceptor for unary calls that adds headers into the outgoing
// metadata headers based on examining the context.
func NewMetadataExtractingAndForwardingUnaryClientInterceptor(metadataExtractor MetadataExtractor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		extraMetadata, err := metadataExtractor(ctx)
		if err != nil {
			return util.StatusWrap(err, "Failed to extract metadata")
		}
		return invoker(metadata.AppendToOutgoingContext(ctx, extraMetadata...), method, req, resp, cc, opts...)
	}
}

// NewMetadataExtractingAndForwardingStreamClientInterceptor creates a gRPC
// request interceptor for unary calls that adds headers into the outgoing
// metadata headers based on examining the context.
func NewMetadataExtractingAndForwardingStreamClientInterceptor(metadataExtractor MetadataExtractor) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		extraMetadata, err := metadataExtractor(ctx)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to extract metadata")
		}
		return streamer(metadata.AppendToOutgoingContext(ctx, extraMetadata...), desc, cc, method, opts...)
	}
}
