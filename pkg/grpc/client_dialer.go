package grpc

import (
	"github.com/buildbarn/bb-storage/pkg/program"
	"google.golang.org/grpc"
)

// ClientDialer is a function type that corresponds with the prototype
// of grpc.NewClient(). It can be used to override the dialer function
// that is invoked by BaseClientFactory.
//
// While grpc.NewClient() returns a *grpc.ClientConn, this function
// type returns grpc.ClientConnInterface to make it possible to wrap
// gRPC client connections.
type ClientDialer func(terminationGroup program.Group, target string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error)

// BaseClientDialer is a ClientDialer that calls gRPC's
// NewClient() function and closes the connection when terminationGroup
// is cancelled.
var BaseClientDialer = NewClientClosingDialer(leakingClientDialer)

// leakingClientDialer leaves the returned connection open until the
// returned closer is called.
func leakingClientDialer(target string, opts ...grpc.DialOption) (ClientConnCloser, error) {
	return grpc.NewClient(target, opts...)
}
