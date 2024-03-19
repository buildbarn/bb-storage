package grpcauth_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/grpcauth"
	"github.com/buildbarn/bb-storage/pkg/testutil"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDenyAuthenticator(t *testing.T) {
	authenticator := grpcauth.NewDenyAuthenticator("This service has been disabled")
	_, err := authenticator.Authenticate(context.Background())
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unauthenticated, "This service has been disabled"),
		err)
}
