package http_test

import (
	"net/http"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/structpb"

	"go.uber.org/mock/gomock"
)

func TestAllowAuthenticator(t *testing.T) {
	ctrl := gomock.NewController(t)

	expectedMetadata := auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"username": structpb.NewStringValue("John Doe"),
			},
		}),
	})
	authenticator := bb_http.NewAllowAuthenticator(expectedMetadata)

	w := mock.NewMockResponseWriter(ctrl)
	r, err := http.NewRequest(http.MethodGet, "/path", nil)
	require.NoError(t, err)
	actualMetadata, err := authenticator.Authenticate(w, r)
	require.NoError(t, err)
	require.Equal(t, expectedMetadata, actualMetadata)
}
