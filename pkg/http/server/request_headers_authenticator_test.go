package server_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	http_server "github.com/buildbarn/bb-storage/pkg/http/server"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"go.uber.org/mock/gomock"
)

func TestRequestHeadersAuthenticator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backend := mock.NewMockRequestHeadersAuthenticator(ctrl)

	t.Run("BackendSuccess", func(t *testing.T) {
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, "/path", nil)
		require.NoError(t, err)
		r.Header.Set("aUTHORIZATION", "token")
		r.Header["Other-Header"] = []string{"Other", "Other2"}
		r.Header.Set("Unused-Header", "DontUseMe")

		backend.EXPECT().Authenticate(
			ctx, map[string][]string{
				"Authorization": {"token"},
				"Other-Header":  {"Other", "Other2"},
				// MissingHeader is not part of the request.
				// UnusedHeader should not be passed.
			},
		).Return(util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("You're totally who you say you are"),
		})), nil)

		authenticator, err := http_server.NewRequestHeadersAuthenticator(
			backend,
			[]string{
				"Authorization",
				"Other-Header",
				"Missing-Header",
			},
		)
		require.NoError(t, err)

		metadata, err := authenticator.Authenticate(nil, r)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"public": "You're totally who you say you are",
		}, metadata.GetRaw())
	})

	t.Run("BackendFailure", func(t *testing.T) {
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, "/path", nil)
		require.NoError(t, err)
		backend.EXPECT().Authenticate(
			ctx, map[string][]string{},
		).Return(nil, status.Error(codes.Unauthenticated, "Server offline"))

		authenticator, err := http_server.NewRequestHeadersAuthenticator(
			backend,
			[]string{},
		)
		require.NoError(t, err)

		_, err = authenticator.Authenticate(nil, r)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unauthenticated, "Server offline"),
			err)
	})

	// The current implementation forwards headers in canonical form, so don't
	// allow configuring headers in other forms as that may confuse the users.
	t.Run("OnlyAcceptCanonicalHeaders", func(t *testing.T) {
		_, err := http_server.NewRequestHeadersAuthenticator(
			backend,
			[]string{"Non-CANONICAL-Header"},
		)
		testutil.RequireEqualStatus(
			t,
			status.Error(
				codes.InvalidArgument,
				"Header key \"Non-CANONICAL-Header\" is not canonical, did you mean \"Non-Canonical-Header\"?",
			),
			err)
	})
}
