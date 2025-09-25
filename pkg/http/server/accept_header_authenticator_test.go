package server_test

import (
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

func TestAcceptHeaderAuthenticator(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseAuthenticator := mock.NewMockHTTPAuthenticator(ctrl)
	authenticator := http_server.NewAcceptHeaderAuthenticator(baseAuthenticator, []string{"text/html", "font/*"})

	t.Run("MissingHeader", func(t *testing.T) {
		w := mock.NewMockResponseWriter(ctrl)
		r, err := http.NewRequest(http.MethodGet, "/path", nil)
		require.NoError(t, err)

		_, err = authenticator.Authenticate(w, r)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "Client does not accept media types [text/html font/*]"), err)
	})

	t.Run("MismatchingHeader", func(t *testing.T) {
		w := mock.NewMockResponseWriter(ctrl)
		r, err := http.NewRequest(http.MethodGet, "/path", nil)
		require.NoError(t, err)
		r.Header.Set("Accept", "application/xml")

		_, err = authenticator.Authenticate(w, r)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "Client does not accept media types [text/html font/*]"), err)
	})

	t.Run("MatchingHeader", func(t *testing.T) {
		for _, header := range []string{
			"font/otf",
			"text/*",
			"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		} {
			w := mock.NewMockResponseWriter(ctrl)
			r, err := http.NewRequest(http.MethodGet, "/path", nil)
			require.NoError(t, err)
			r.Header.Set("Accept", header)

			expectedMetadata := util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
				Public: structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"username": structpb.NewStringValue("John Doe"),
					},
				}),
			}))
			baseAuthenticator.EXPECT().Authenticate(w, r).Return(expectedMetadata, nil)

			actualMetadata, err := authenticator.Authenticate(w, r)
			require.NoError(t, err)
			require.Equal(t, expectedMetadata, actualMetadata)
		}
	})
}
