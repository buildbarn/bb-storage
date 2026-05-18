package grpc_test

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

// TestRoundRobinClientConnSingleReturnsEntry confirms the
// len(conns)==1 fast-path returns the entry directly with no wrapping
// overhead — important because the pool-size=1 case is the default and
// must not pay any cost.
func TestRoundRobinClientConnSingleReturnsEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	only := mock.NewMockClientConnInterface(ctrl)
	conn := bb_grpc.NewRoundRobinClientConn([]grpc.ClientConnInterface{only})
	// Identity is the contract: the lone entry IS the returned value.
	if conn != grpc.ClientConnInterface(only) {
		t.Fatalf("len=1 pool did not return the entry directly: got %T", conn)
	}
}

// TestRoundRobinClientConnInvokeDistributes proves Invoke distributes
// across all N conns evenly when called N×k times.
func TestRoundRobinClientConnInvokeDistributes(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	const N, k = 4, 25 // 100 calls total, 25 each
	conns := make([]grpc.ClientConnInterface, N)
	for i := 0; i < N; i++ {
		m := mock.NewMockClientConnInterface(ctrl)
		m.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(k).Return(nil)
		conns[i] = m
	}
	conn := bb_grpc.NewRoundRobinClientConn(conns)
	for i := 0; i < N*k; i++ {
		if err := conn.Invoke(ctx, "/test/Method", nil, nil); err != nil {
			t.Fatalf("Invoke %d: %v", i, err)
		}
	}
	// gomock verifies the Times(k) expectations on Finish — invocations
	// off-balance by even one would fail the controller.
}

// TestRoundRobinClientConnNewStreamDistributes mirrors the Invoke
// test for streaming RPCs.
func TestRoundRobinClientConnNewStreamDistributes(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	const N, k = 3, 30
	conns := make([]grpc.ClientConnInterface, N)
	for i := 0; i < N; i++ {
		m := mock.NewMockClientConnInterface(ctrl)
		m.EXPECT().NewStream(gomock.Any(), gomock.Any(), gomock.Any()).Times(k).Return(nil, nil)
		conns[i] = m
	}
	conn := bb_grpc.NewRoundRobinClientConn(conns)
	for i := 0; i < N*k; i++ {
		if _, err := conn.NewStream(ctx, nil, "/test/Stream"); err != nil {
			t.Fatalf("NewStream %d: %v", i, err)
		}
	}
}

// TestRoundRobinClientConnConcurrent proves the atomic counter is
// race-free under concurrent dispatch and distribution stays balanced
// modulo the natural N-1 slack from goroutine scheduling. Runs under
// -race to catch any non-atomic mutation of the counter.
func TestRoundRobinClientConnConcurrent(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	const N, callsPerGoroutine = 8, 250
	const goroutines = 16
	total := goroutines * callsPerGoroutine
	expectedPer := total / N // 500

	counts := make([]int, N)
	var mu sync.Mutex
	conns := make([]grpc.ClientConnInterface, N)
	for i := 0; i < N; i++ {
		idx := i
		m := mock.NewMockClientConnInterface(ctrl)
		m.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, string, interface{}, interface{}, ...grpc.CallOption) error {
				mu.Lock()
				counts[idx]++
				mu.Unlock()
				return nil
			}).AnyTimes()
		conns[i] = m
	}
	conn := bb_grpc.NewRoundRobinClientConn(conns)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < callsPerGoroutine; i++ {
				_ = conn.Invoke(ctx, "/test/Method", nil, nil)
			}
		}()
	}
	wg.Wait()

	// Exact balance: every conn got `total/N` calls. The atomic counter
	// is monotonically incremented, so the modulo distribution is
	// deterministic regardless of goroutine interleaving.
	for i, got := range counts {
		if got != expectedPer {
			t.Errorf("conn[%d] got %d calls; want exactly %d", i, got, expectedPer)
		}
	}
}
