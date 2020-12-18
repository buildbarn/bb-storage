package grpc

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/atomic"
	"github.com/buildbarn/bb-storage/pkg/util"

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
		}, nil
	}
}

type lazyClientConn struct {
	dialer      ClientDialer
	target      string
	dialOptions []grpc.DialOption

	state      atomic.Uint32
	wakeup     chan struct{}
	connection grpc.ClientConnInterface
	lock       sync.Mutex
}

func (cc *lazyClientConn) openConnection(ctx context.Context) (grpc.ClientConnInterface, error) {
	// Fast path: immediately return the existing connection that
	// was created previously.
	if cc.state.Load() == 2 {
		return cc.connection, nil
	}

	// Slow path: initialize or wait for initialization by another
	// goroutine to complete.
	for {
		cc.lock.Lock()
		switch cc.state.Load() {
		case 0:
			// Connection is uninitialized. Attempt to
			// create it ourselves.
			cc.wakeup = make(chan struct{}, 1)
			cc.state.Store(1)
			cc.lock.Unlock()

			conn, err := cc.dialer(ctx, cc.target, cc.dialOptions...)

			cc.lock.Lock()
			close(cc.wakeup)
			if err != nil {
				// Creation failed. Return to the
				// initial state.
				cc.state.Store(0)
				cc.lock.Unlock()
				return nil, util.StatusWrap(err, "Failed to create client connection")
			}

			// Creation succeeded.
			cc.state.Store(2)
			cc.connection = conn
			cc.lock.Unlock()
			return conn, nil
		case 1:
			// Another goroutine is attempting to
			// initialize. Wait for that to complete.
			ch := cc.wakeup
			cc.lock.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return nil, util.StatusFromContext(ctx)
			}
		case 2:
			// Another goroutine completed initialization.
			cc.lock.Unlock()
			return cc.connection, nil
		}
	}
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
