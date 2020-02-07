package grpc_test

import (
	"context"
	"testing"

	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDenyAuthenticator(t *testing.T) {
	authenticator := bb_grpc.NewDenyAuthenticator("This service has been disabled")
	require.Equal(
		t,
		status.Error(codes.Unauthenticated, "This service has been disabled"),
		authenticator.Authenticate(context.Background()))
}
