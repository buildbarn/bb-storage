package grpc

import (
	"context"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
)

// NewLazyClientDialer creates a gRPC ClientDialer that forwards calls
// to an underlying implementation. Instead of creating a gRPC client
// connection immediately, the creation is delayed until the first RPC
// is invoked.
//
// The purpose of this decorator is to ensure that gRPC client
// connections are only opened if we actually use it to send RPCs. Doing
// this is especially important for users of DemultiplexingBlobAccess
// and DemultiplexingBuildQueue, as it's often the case that one or more
// backends are never used.
//
// In an ideal world, a feature like this would be part of gRPC itself,
// together with the ability to automatically hang up connections if
// unused for a prolonged amount of time.
func NewLazyClientDialer(dialer ClientDialer) ClientDialer {
	return func(ctx context.Context, target string, dialOptions ...grpc.DialOption) (grpc.ClientConnInterface, error) {
		return &lazyClientConn{
			dialer:      dialer,
			target:      target,
			dialOptions: dialOptions,

			initializationSemaphore: semaphore.NewWeighted(1),
		}, nil
	}
}

type lazyClientConn struct {
	dialer      ClientDialer
	target      string
	dialOptions []grpc.DialOption

	connection              atomic.Pointer[grpc.ClientConnInterface]
	initializationSemaphore *semaphore.Weighted
}

func (cc *lazyClientConn) openConnection(ctx context.Context) (grpc.ClientConnInterface, error) {
	// Fast path: immediately return the existing connection that
	// was created previously.
	if conn := cc.connection.Load(); conn != nil {
		return *conn, nil
	}

	// Slow path: initialize or wait for initialization by another
	// goroutine to complete.
	if cc.initializationSemaphore.Acquire(ctx, 1) != nil {
		return nil, util.StatusFromContext(ctx)
	}
	defer cc.initializationSemaphore.Release(1)

	if conn := cc.connection.Load(); conn != nil {
		return *conn, nil
	}
	conn, err := cc.dialer(ctx, cc.target, cc.dialOptions...)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create client connection")
	}
	cc.connection.Store(&conn)
	return conn, nil
}

func (cc *lazyClientConn) Invoke(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
	conn, err := cc.openConnection(ctx)
	if err != nil {
		return err
	}
	return conn.Invoke(ctx, method, args, reply, opts...)
}

func (cc *lazyClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, err := cc.openConnection(ctx)
	if err != nil {
		return nil, err
	}
	return conn.NewStream(ctx, desc, method, opts...)
}
