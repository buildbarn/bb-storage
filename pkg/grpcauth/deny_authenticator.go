package grpcauth

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type denyAuthenticator struct {
	err error
}

// NewDenyAuthenticator creates an Authenticator that always returns an
// UNAUTHENTICATED error with a fixed error message string. This
// implementation can be used in case a gRPC server needs to be
// administratively disabled without shutting it down entirely.
func NewDenyAuthenticator(message string) Authenticator {
	return &denyAuthenticator{
		err: status.Error(codes.Unauthenticated, message),
	}
}

func (a denyAuthenticator) Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error) {
	return nil, a.err
}
