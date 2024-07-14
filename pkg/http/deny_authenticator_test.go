package http_test

import (
	"net/http"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestDenyAuthenticator(t *testing.T) {
	ctrl := gomock.NewController(t)

	authenticator := bb_http.NewDenyAuthenticator("This service has been disabled")

	w := mock.NewMockResponseWriter(ctrl)
	r, err := http.NewRequest(http.MethodGet, "/path", nil)
	require.NoError(t, err)
	_, err = authenticator.Authenticate(w, r)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unauthenticated, "This service has been disabled"),
		err)
}
