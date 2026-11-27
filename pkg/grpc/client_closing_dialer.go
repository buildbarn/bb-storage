package grpc

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc"
)

// ClientConnCloser extends grpc.ClientConnInterface to be possible to
// close.
type ClientConnCloser interface {
	grpc.ClientConnInterface
	io.Closer
}

// NewClientClosingDialer creates a factory for gRPC client connections
// where the created connections are closed when the factory provided
// terminationGroup is cancelled.
//
// When using grpc.NewClient(), the caller has to close the connection
// manually. This function automates that process by calling Close() on
// the connection returned by clientCloserDialer.
func NewClientClosingDialer(clientCloserDialer func(target string, opts ...grpc.DialOption) (ClientConnCloser, error)) ClientDialer {
	return func(terminationGroup program.Group, target string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
		clientConn, err := clientCloserDialer(target, opts...)
		if err == nil {
			terminationGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				<-ctx.Done()
				if err := clientConn.Close(); err != nil {
					return util.StatusWrapf(err, "Failed to close gRPC client connection to %v", target)
				}
				return nil
			})
		}
		return clientConn, err
	}
}
