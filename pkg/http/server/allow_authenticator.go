package server

import (
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/auth"
)

type allowAuthenticator struct {
	metadata *auth.AuthenticationMetadata
}

// NewAllowAuthenticator creates an implementation of Authenticator that
// simply always returns success. This implementation can be used in
// case a HTTP server needs to be started that does not perform any
// authentication.
func NewAllowAuthenticator(metadata *auth.AuthenticationMetadata) Authenticator {
	return allowAuthenticator{
		metadata: metadata,
	}
}

func (a allowAuthenticator) Authenticate(w http.ResponseWriter, r *http.Request) (*auth.AuthenticationMetadata, error) {
	return a.metadata, nil
}
