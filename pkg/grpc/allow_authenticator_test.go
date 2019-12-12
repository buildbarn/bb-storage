package grpc_test

import (
	"context"
	"testing"

	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/stretchr/testify/require"
)

func TestAllowAuthenticator(t *testing.T) {
	require.NoError(t, bb_grpc.AllowAuthenticator.Authenticate(context.Background()))
}
