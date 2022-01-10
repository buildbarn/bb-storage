package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"google.golang.org/grpc"
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
		return handler(context.WithValue(ctx, auth.AuthenticationMetadata{}, metadata), req)
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
		wrappedServerStream := grpc_middleware.WrapServerStream(ss)
		wrappedServerStream.WrappedContext = context.WithValue(ctx, auth.AuthenticationMetadata{}, metadata)

		return handler(srv, wrappedServerStream)
	}
}
