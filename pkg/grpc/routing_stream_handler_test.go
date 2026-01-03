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
	streamHandler := mock.NewMockStreamHandler(ctrl)

	forwarder := bb_grpc.NewRoutingStreamHandler(map[string]grpc.StreamHandler{
		"serviceA":  streamHandler.Call,
		"/serviceB": streamHandler.Call,
	})

	serverTransportStream.EXPECT().Method().Return("/serviceA/method1")
	streamHandler.EXPECT().Call(someSrv, incomingStream).Return(errors.New("called"))
	require.Error(t, forwarder(someSrv, incomingStream), "called")

	serverTransportStream.EXPECT().Method().Return("/serviceB/method2")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unimplemented, "No route for service serviceB"),
		forwarder(someSrv, incomingStream),
	)

	serverTransportStream.EXPECT().Method().Return("/non.existing/service/bad-method")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unimplemented, "No route for service non.existing/service"),
		forwarder(someSrv, incomingStream),
	)
	serverTransportStream.EXPECT().Method().Return("non.existing/service/bad-method")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unimplemented, "No route for service non.existing/service"),
		forwarder(someSrv, incomingStream),
	)

	serverTransportStream.EXPECT().Method().Return("/service.only")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.InvalidArgument, "Malformed method name /service.only"),
		forwarder(someSrv, incomingStream),
	)
	serverTransportStream.EXPECT().Method().Return("service.only")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.InvalidArgument, "Malformed method name service.only"),
		forwarder(someSrv, incomingStream),
	)
	serverTransportStream.EXPECT().Method().Return("/")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.InvalidArgument, "Malformed method name /"),
		forwarder(someSrv, incomingStream),
	)
	serverTransportStream.EXPECT().Method().Return("")
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.InvalidArgument, "Malformed method name "),
		forwarder(someSrv, incomingStream),
	)
}
