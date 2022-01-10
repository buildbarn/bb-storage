package grpc

import (
	"context"
)

type allowAuthenticator struct {
	metadata interface{}
}

// NewAllowAuthenticator creates an implementation of Authenticator that
// simply always returns success. This implementation can be used in
// case a gRPC server needs to be started that does not perform any
// authentication (e.g., one listening on a UNIX socket with restricted
// file permissions).
func NewAllowAuthenticator(metadata interface{}) Authenticator {
	return allowAuthenticator{
		metadata: metadata,
	}
}

func (a allowAuthenticator) Authenticate(ctx context.Context) (interface{}, error) {
	return a.metadata, nil
}
