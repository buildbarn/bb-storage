package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// ClientDialer is a function type that corresponds with the prototype
// of grpc.DialContext(). It can be used to override the dialer function
// that is invoked by BaseClientFactory.
//
// While grpc.DialContext() returns a *grpc.ClientConn, this function
// type returns grpc.ClientConnInterface to make it possible to wrap
// gRPC client connections.
type ClientDialer func(ctx context.Context, target string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error)

// BaseClientDialer is a ClientDialer that simply calls gRPC's
// DialContext() function.
func BaseClientDialer(ctx context.Context, target string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	return grpc.DialContext(ctx, target, opts...)
}
