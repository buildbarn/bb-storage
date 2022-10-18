package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type peerCredentialsAuthenticator struct {
	metadataExtractor *jmespath.JMESPath
}

// NewPeerCredentialsAuthenticator creates an Authenticator that only
// grants access in case the client connected to the gRPC server over a
// UNIX socket. The credentials (user ID, group memberships) of the
// client may be added to the authentication metadata.
func NewPeerCredentialsAuthenticator(metadataExtractor *jmespath.JMESPath) Authenticator {
	return &peerCredentialsAuthenticator{
		metadataExtractor: metadataExtractor,
	}
}

func (a *peerCredentialsAuthenticator) Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error) {
	// Extract peer credentials from the connection.
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "Connection was not established using gRPC")
	}
	authInfo, ok := p.AuthInfo.(PeerAuthInfo)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "Connection was not established over a UNIX socket")
	}

	// Convert peer credentials to authentication metadata.
	groups := make([]any, 0, len(authInfo.Groups))
	for _, group := range authInfo.Groups {
		groups = append(groups, group)
	}
	metadataRaw, err := a.metadataExtractor.Search(map[string]any{
		"uid":    authInfo.UID,
		"groups": groups,
	})
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot extract metadata from peer credentials")
	}
	return auth.NewAuthenticationMetadataFromRaw(metadataRaw)
}
