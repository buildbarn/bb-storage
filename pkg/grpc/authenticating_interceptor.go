package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"google.golang.org/grpc"

	"go.opentelemetry.io/otel/trace"
)

// NewAuthenticatingUnaryInterceptor creates a gRPC request interceptor
// for unary calls that passes all requests through an Authenticator.
// This may be used to enable authentication support on a gRPC server.
func NewAuthenticatingUnaryInterceptor(a Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		metadata, err := a.Authenticate(ctx)
		if err != nil {
			return nil, err
		}
		trace.SpanFromContext(ctx).SetAttributes(metadata.GetTracingAttributes()...)
		return handler(auth.NewContextWithAuthenticationMetadata(ctx, metadata), req)
	}
}

// NewAuthenticatingStreamInterceptor creates a gRPC request interceptor
// for streaming calls that passes all requests through an
// Authenticator. This may be used to enable authentication support on a
// gRPC server.
func NewAuthenticatingStreamInterceptor(a Authenticator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		metadata, err := a.Authenticate(ctx)
		if err != nil {
			return err
		}
		trace.SpanFromContext(ctx).SetAttributes(metadata.GetTracingAttributes()...)
		wrappedServerStream := grpc_middleware.WrapServerStream(ss)
		wrappedServerStream.WrappedContext = auth.NewContextWithAuthenticationMetadata(ctx, metadata)

		return handler(srv, wrappedServerStream)
	}
}
