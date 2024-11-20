package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type remoteRequestAuthenticator struct {
	remoteAuthenticator auth.RequestHeadersAuthenticator
	headerKeys          []string
}

// NewRemoteRequestAuthenticator creates a new Authenticator for incoming gRPC
// requests that forwards configured headers to a remote service for
// authentication. The result from the remote service is cached.
func NewRemoteRequestAuthenticator(
	remoteAuthenticator auth.RequestHeadersAuthenticator,
	headerKeys []string,
) Authenticator {
	return &remoteRequestAuthenticator{
		remoteAuthenticator: remoteAuthenticator,
		headerKeys:          headerKeys,
	}
}

func (a *remoteRequestAuthenticator) Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error) {
	metadata, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "Not called from within an incoming gRPC context")
	}
	requestHeaders := make(map[string][]string, len(a.headerKeys))
	for _, key := range a.headerKeys {
		if values := metadata.Get(key); len(values) != 0 {
			requestHeaders[key] = values
		}
	}
	return a.remoteAuthenticator.Authenticate(ctx, requestHeaders)
}
