package grpc_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

// TestConnectionPoolDialsNTimes verifies that a ConnectionPoolSize=N
// configuration causes the factory to dial the same target N times,
// each producing an independent grpc.ClientConnInterface. This is the
// core property the production fix relies on: pool=8 must give us 8
// independent HTTP/2 sessions to a single backend, not 1 session reused.
func TestConnectionPoolDialsNTimes(t *testing.T) {
	for _, poolSize := range []int32{0, 1, 2, 8, 32} {
		poolSize := poolSize
		t.Run(name(poolSize), func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)

			// Each call to the underlying dialer returns a fresh
			// mock conn, and the expected total call count equals
			// max(poolSize, 1) — pool sizes 0 and 1 both yield a
			// single dial because pool=0 is the back-compat default.
			expected := int(poolSize)
			if expected < 1 {
				expected = 1
			}
			var dialCount atomic.Int32
			dialer := func(_ context.Context, target string, _ ...grpc.DialOption) (grpc.ClientConnInterface, error) {
				dialCount.Add(1)
				return mock.NewMockClientConnInterface(ctrl), nil
			}

			factory := bb_grpc.NewBaseClientFactory(dialer, nil, nil, nil)
			conn, err := factory.NewClientFromConfiguration(&configuration.ClientConfiguration{
				Address:            "example.com:9092",
				ConnectionPoolSize: poolSize,
			}, nil)
			if err != nil {
				t.Fatalf("NewClientFromConfiguration: %v", err)
			}
			if conn == nil {
				t.Fatal("expected non-nil ClientConnInterface")
			}
			if got := int(dialCount.Load()); got != expected {
				t.Errorf("ConnectionPoolSize=%d: dialer invoked %d times; want %d", poolSize, got, expected)
			}
			_ = ctx
		})
	}
}

// TestConnectionPoolRoundRobinAcrossDials proves that after the factory
// dials N times, each subsequent Invoke is routed to a distinct
// underlying conn (round-robin). This is the property that makes the
// pool actually deliver parallelism: streams from N concurrent callers
// land on N distinct HTTP/2 sessions, not all on the first one.
func TestConnectionPoolRoundRobinAcrossDials(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	const N = 4
	mocks := make([]*mock.MockClientConnInterface, N)
	var dialIdx atomic.Int32
	dialer := func(_ context.Context, _ string, _ ...grpc.DialOption) (grpc.ClientConnInterface, error) {
		i := dialIdx.Add(1) - 1
		m := mock.NewMockClientConnInterface(ctrl)
		mocks[i] = m
		return m, nil
	}

	// Each conn should see exactly 3 calls (12 total / 4 conns).
	for _, m := range []*mock.MockClientConnInterface{} {
		_ = m
	}

	factory := bb_grpc.NewBaseClientFactory(dialer, nil, nil, nil)
	conn, err := factory.NewClientFromConfiguration(&configuration.ClientConfiguration{
		Address:            "example.com:9092",
		ConnectionPoolSize: N,
	}, nil)
	if err != nil {
		t.Fatalf("NewClientFromConfiguration: %v", err)
	}

	// Set the expectations after dialing so we have references to the
	// per-iteration mocks.
	for _, m := range mocks {
		m.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Times(3).Return(nil)
	}

	// 12 calls round-robin'd across 4 conns: each conn gets exactly 3.
	for i := 0; i < 4*3; i++ {
		if err := conn.Invoke(ctx, "/test/Method", nil, nil); err != nil {
			t.Fatalf("Invoke %d: %v", i, err)
		}
	}
}

// TestConnectionPoolConcurrentDispatch exercises the round-robin
// counter under concurrent callers. Runs under -race; any non-atomic
// mutation of the counter would surface here.
func TestConnectionPoolConcurrentDispatch(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	const N = 8
	const callsPerGoroutine = 200
	const goroutines = 16
	total := goroutines * callsPerGoroutine

	mocks := make([]*mock.MockClientConnInterface, N)
	for i := 0; i < N; i++ {
		mocks[i] = mock.NewMockClientConnInterface(ctrl)
		// Expect total/N calls per conn — deterministic since the
		// counter is monotonic.
		mocks[i].EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Times(total / N).Return(nil)
	}
	var i atomic.Int32
	dialer := func(_ context.Context, _ string, _ ...grpc.DialOption) (grpc.ClientConnInterface, error) {
		return mocks[int(i.Add(1)-1)], nil
	}

	factory := bb_grpc.NewBaseClientFactory(dialer, nil, nil, nil)
	conn, err := factory.NewClientFromConfiguration(&configuration.ClientConfiguration{
		Address:            "example.com:9092",
		ConnectionPoolSize: N,
	}, nil)
	if err != nil {
		t.Fatalf("NewClientFromConfiguration: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				_ = conn.Invoke(ctx, "/test/Method", nil, nil)
			}
		}()
	}
	wg.Wait()
}

func name(poolSize int32) string {
	switch poolSize {
	case 0:
		return "Default_0_DialsOnce"
	case 1:
		return "Explicit_1_DialsOnce"
	default:
		switch poolSize {
		case 2:
			return "Pool_2"
		case 8:
			return "Pool_8"
		case 32:
			return "Pool_32"
		}
		return "Pool_other"
	}
}
