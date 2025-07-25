package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/jmespath"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestPeerCredentialsAuthenticator(t *testing.T) {
	ctx := context.Background()

	authenticator := grpc.NewPeerCredentialsAuthenticator(jmespath.MustCompile("{\"public\": @}"))

	t.Run("NoGRPC", func(t *testing.T) {
		// Authenticator is used outside of gRPC, meaning it cannot
		// extract peer state information.
		_, err := authenticator.Authenticate(ctx)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "Connection was not established using gRPC"), err)
	})

	t.Run("NoPeerAuthInfo", func(t *testing.T) {
		// Connection that was not established over a UNIX socket.
		_, err := authenticator.Authenticate(peer.NewContext(ctx, &peer.Peer{}))
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "Connection was not established over a UNIX socket"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Connection that was established over a UNIX socket.
		actualMetadata, err := authenticator.Authenticate(
			peer.NewContext(
				ctx,
				&peer.Peer{
					AuthInfo: grpc.PeerAuthInfo{
						UID:    1000,
						Groups: []uint32{100, 12, 42},
					},
				}))
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"public": map[string]any{
				"uid":    1000.0,
				"groups": []any{100.0, 12.0, 42.0},
			},
		}, actualMetadata.GetRaw())
	})
}
