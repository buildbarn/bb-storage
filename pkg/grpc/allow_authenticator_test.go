package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/stretchr/testify/require"
)

func TestAllowAuthenticator(t *testing.T) {
	expectedMetadata := auth.MustNewAuthenticationMetadata(map[string]interface{}{"username": "John Doe"})
	a := bb_grpc.NewAllowAuthenticator(expectedMetadata)
	actualMetadata, err := a.Authenticate(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedMetadata, actualMetadata)
}
