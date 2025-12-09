package grpc_test

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestRoutingStreamForwarder(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	someSrv := "server"
	serverTransportStream := mock.NewMockServerTransportStream(ctrl)
	streamCtx := grpc.NewContextWithServerTransportStream(ctx, serverTransportStream)
	incomingStream := mock.NewMockServerStream(ctrl)
	incomingStream.EXPECT().Context().Return(streamCtx).AnyTimes()

	// The test assumes that the incomingStream is forwarded straight through
	// the RoutingStreamForwarder, even if the implementation is allowed to do
	// some wrapping.
	forwarder := bb_grpc.NewRoutingStreamForwarder()
	forwarder.RouteTable["/serviceA/method1"] = func(srv any, stream grpc.ServerStream) error {
		require.Equal(t, srv, someSrv)
		require.Equal(t, stream, incomingStream)
		return errors.New("A1")
	}
	forwarder.RouteTable["generic-service-method-name"] = func(srv any, stream grpc.ServerStream) error {
		require.Equal(t, srv, someSrv)
		require.Equal(t, stream, incomingStream)
		return errors.New("generic")
	}

	serverTransportStream.EXPECT().Method().Return("/serviceA/method1")
	require.Error(t, forwarder.HandleStream(someSrv, incomingStream), "A1")

	serverTransportStream.EXPECT().Method().Return("generic-service-method-name")
	require.Error(t, forwarder.HandleStream(someSrv, incomingStream), "generic")

	serverTransportStream.EXPECT().Method().Return("/non-existing-service/bad-method")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unimplemented, "No route for method /non-existing-service/bad-method"),
		forwarder.HandleStream(someSrv, incomingStream),
	)
}
