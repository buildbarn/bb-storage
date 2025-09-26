package server

import (
	"net/http"

	"github.com/aohorodnyk/mimeheader"
	"github.com/buildbarn/bb-storage/pkg/auth"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type acceptHeaderAuthenticator struct {
	base        Authenticator
	mediaTypes  []string
	mismatchErr error
}

// NewAcceptHeaderAuthenticator creates a decorator for Authenticator
// that only performs authentication if the HTTP request's "Accept" header
// contains a matching media type. This can, for example, be used to
// limit OpenID Connect authentication to requests originating from a
// web browser.
func NewAcceptHeaderAuthenticator(base Authenticator, mediaTypes []string) Authenticator {
	return &acceptHeaderAuthenticator{
		base:        base,
		mediaTypes:  mediaTypes,
		mismatchErr: status.Errorf(codes.Unauthenticated, "Client does not accept media types %v", mediaTypes),
	}
}

func (a *acceptHeaderAuthenticator) Authenticate(w http.ResponseWriter, r *http.Request) (*auth.AuthenticationMetadata, error) {
	acceptHeader := mimeheader.ParseAcceptHeader(r.Header.Get("Accept"))
	if _, _, ok := acceptHeader.Negotiate(a.mediaTypes, ""); ok {
		return a.base.Authenticate(w, r)
	}
	return nil, a.mismatchErr
}
