package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMetadataForwardingAndReusingInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	interceptor := bb_grpc.NewMetadataForwardingAndReusingInterceptor([]string{"a", "b"})

	testUnaryClientCall := func(ctx context.Context, expectedMD map[string]string, err error) {
		invoker := mock.NewMockUnaryInvoker(ctrl)
		req := &empty.Empty{}
		resp := &empty.Empty{}
		invoker.EXPECT().Call(gomock.Any(), "SomeMethod", req, resp, nil).DoAndReturn(
			func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				actualMD, ok := metadata.FromOutgoingContext(ctx)
				require.True(t, ok)
				require.Equal(t, metadata.New(expectedMD), actualMD)
				return err
			})
		require.Equal(
			t,
			err,
			interceptor.InterceptUnaryClient(ctx, "SomeMethod", req, resp, nil, invoker.Call))
	}

	// No headers should be added in the initial case.
	testUnaryClientCall(ctx, map[string]string{}, nil)

	// Headers not specified in the constructor should not be
	// forwarded. They should also not be present in future calls.
	testUnaryClientCall(
		metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{"hello": "world"})),
		map[string]string{},
		nil)
	testUnaryClientCall(ctx, map[string]string{}, nil)

	// Matching headers should be forwarded. Because the call to the
	// backend fails, the header is not reused in future calls.
	testUnaryClientCall(
		metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{"a": "aardvark"})),
		map[string]string{"a": "aardvark"},
		status.Error(codes.Internal, "Backend failure"))
	testUnaryClientCall(ctx, map[string]string{}, nil)

	// Backend the call to the backend succeeds, the header value is
	// reused in future calls.
	testUnaryClientCall(
		metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{"a": "aardvark"})),
		map[string]string{"a": "aardvark"},
		nil)
	testUnaryClientCall(ctx, map[string]string{"a": "aardvark"}, nil)

	// Each of the provided headers should be managed independently.
	// Specifying the second header causes both headers to be sent.
	testUnaryClientCall(
		metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{"b": "buffalo"})),
		map[string]string{"a": "aardvark", "b": "buffalo"},
		nil)
	testUnaryClientCall(ctx, map[string]string{"a": "aardvark", "b": "buffalo"}, nil)

	// The existing value should be retained in case of backend
	// failures.
	testUnaryClientCall(
		metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{"a": "albatross"})),
		map[string]string{"a": "albatross", "b": "buffalo"},
		status.Error(codes.Internal, "Backend failure"))
	testUnaryClientCall(ctx, map[string]string{"a": "aardvark", "b": "buffalo"}, nil)

	// Succeeding calls should cause existing header values to be
	// overwritten.
	testUnaryClientCall(
		metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{"a": "albatross"})),
		map[string]string{"a": "albatross", "b": "buffalo"},
		nil)
	testUnaryClientCall(ctx, map[string]string{"a": "albatross", "b": "buffalo"}, nil)
}
