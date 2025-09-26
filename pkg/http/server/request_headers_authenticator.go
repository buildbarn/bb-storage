package server

import (
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type requestHeadersAuthenticator struct {
	backend    auth.RequestHeadersAuthenticator
	headerKeys []string
}

// NewRequestHeadersAuthenticator creates a new Authenticator for incoming gRPC
// requests that forwards configured headers to a remote service for
// authentication. The result from the remote service is cached.
func NewRequestHeadersAuthenticator(
	backend auth.RequestHeadersAuthenticator,
	headerKeys []string,
) (Authenticator, error) {
	for _, key := range headerKeys {
		if canonicalHeaderKey := http.CanonicalHeaderKey(key); canonicalHeaderKey != key {
			return nil, status.Errorf(codes.InvalidArgument, "Header key %#v is not canonical, did you mean %#v?", key, canonicalHeaderKey)
		}
	}
	return &requestHeadersAuthenticator{
		backend:    backend,
		headerKeys: headerKeys,
	}, nil
}

func (a *requestHeadersAuthenticator) Authenticate(w http.ResponseWriter, r *http.Request) (*auth.AuthenticationMetadata, error) {
	requestHeaders := make(map[string][]string, len(a.headerKeys))
	for _, key := range a.headerKeys {
		if values, ok := r.Header[key]; ok {
			requestHeaders[key] = values
		}
	}
	return a.backend.Authenticate(r.Context(), requestHeaders)
}
