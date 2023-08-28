package http

import (
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/jwt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type jwtAuthenticator struct {
	authorizationHeaderParser *jwt.AuthorizationHeaderParser
}

// NewJWTAuthenticator creates an authenticator for incoming HTTP
// requests that validates requests that contain an "Authorization" of
// shape "Bearer ${jwt}", where ${jwt} is a valid JSON Web Token.
func NewJWTAuthenticator(authorizationHeaderParser *jwt.AuthorizationHeaderParser) Authenticator {
	return &jwtAuthenticator{
		authorizationHeaderParser: authorizationHeaderParser,
	}
}

func (a *jwtAuthenticator) Authenticate(w http.ResponseWriter, r *http.Request) (*auth.AuthenticationMetadata, error) {
	metadata, ok := a.authorizationHeaderParser.ParseAuthorizationHeaders(r.Header["Authorization"])
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "No valid authorization header containing a bearer token provided")
	}
	return metadata, nil
}
