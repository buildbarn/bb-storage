package grpc

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
)

// roundRobinClientConn implements grpc.ClientConnInterface over N
// independent backing connections, distributing each Invoke and
// NewStream call across them in a round-robin order.
//
// The motivation is HTTP/2 head-of-line blocking. Each gRPC client
// connection is one TCP connection carrying one HTTP/2 session, so all
// streams multiplexed on it share the same per-connection flow-control
// window and the same per-stream scheduler queue. Under bursts of
// concurrent reads of large blobs that all land on the same backend
// (e.g., when deterministic sharding sends every read of a particular
// digest to the same shard), the streams serialise behind whichever
// one happens to be ahead in the queue. Pooling N independent
// connections lets up to N streams make progress in parallel.
//
// Selection is per-call (each Invoke / NewStream picks one), not
// per-stream-lifetime. That keeps the wrapping cheap (a single atomic
// add) and avoids any sticky-binding pathology when one backend
// connection wedges.
type roundRobinClientConn struct {
	conns []grpc.ClientConnInterface
	next  atomic.Uint64
}

// NewRoundRobinClientConn returns a grpc.ClientConnInterface that
// dispatches each call across conns in round-robin order. When conns
// has length 1, that single entry is returned directly so the common
// pool=1 path pays zero wrapping overhead.
func NewRoundRobinClientConn(conns []grpc.ClientConnInterface) grpc.ClientConnInterface {
	if len(conns) == 1 {
		return conns[0]
	}
	return &roundRobinClientConn{conns: conns}
}

func (r *roundRobinClientConn) pick() grpc.ClientConnInterface {
	// atomic.Add returns the post-increment value; subtract 1 to get a
	// stable 0..N-1 index sequence across all goroutines.
	n := r.next.Add(1) - 1
	return r.conns[n%uint64(len(r.conns))]
}

func (r *roundRobinClientConn) Invoke(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
	return r.pick().Invoke(ctx, method, args, reply, opts...)
}

func (r *roundRobinClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return r.pick().NewStream(ctx, desc, method, opts...)
}
