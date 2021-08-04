package grpc_test

import (
	"context"
	"testing"

	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/stretchr/testify/require"
)

func TestAllowAuthenticator(t *testing.T) {
	newCtx, err := bb_grpc.AllowAuthenticator.Authenticate(context.Background())
	require.NoError(t, err)
	require.Equal(t, context.Background(), newCtx)
}
