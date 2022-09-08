package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/jwt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type jwtAuthenticator struct {
	authorizationHeaderParser *jwt.AuthorizationHeaderParser
}

// NewJWTAuthenticator creates an authenticator for incoming gRPC
// requests that validates requests that contain an "Authorization" of
// shape "Bearer ${jwt}", where ${jwt} is a valid JSON Web Token.
func NewJWTAuthenticator(authorizationHeaderParser *jwt.AuthorizationHeaderParser) Authenticator {
	return &jwtAuthenticator{
		authorizationHeaderParser: authorizationHeaderParser,
	}
}

func (a *jwtAuthenticator) Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error) {
	grpcMetadata, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "Not called from within an incoming gRPC context")
	}
	metadata, ok := a.authorizationHeaderParser.ParseAuthorizationHeaders(grpcMetadata.Get("authorization"))
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "No valid authorization header containing a bearer token provided")
	}
	return metadata, nil
}
