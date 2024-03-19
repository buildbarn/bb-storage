package grpcauth

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"

	"google.golang.org/protobuf/proto"
)

type allAuthenticator struct {
	authenticators []Authenticator
}

// NewAllAuthenticator wraps a series of Authenticators into a single
// instance.
//
// Allows incoming requests if all the authentication policies allows it.
//
// The authentication metadata is merged, first to last.
// So if this was configured with two authenticators, one which returned
// metadata:
//
//	{
//	    "public": {
//	        "first_name": "kermit",
//	        "middle_name": "the"
//	    }
//	}
//
// And the second returned metadata:
//
//	{
//	    "public": {
//	        "first_name": "robin",
//	        "last_name": "frog"
//	    }
//	}
//
// This authenticator would return the metadata:
//
//	{
//	    "public": {
//	        "first_name": "robin",
//	        "middle_name": "the",
//	        "last_name": "frog"
//	    }
//	}
func NewAllAuthenticator(authenticators []Authenticator) Authenticator {
	switch len(authenticators) {
	case 1:
		return authenticators[0]
	default:
		return &allAuthenticator{
			authenticators: authenticators,
		}
	}
}

func (a *allAuthenticator) Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error) {
	var metadata auth_pb.AuthenticationMetadata

	for _, authenticator := range a.authenticators {
		newMetadata, err := authenticator.Authenticate(ctx)
		if err != nil {
			return nil, err
		}
		fullProto := newMetadata.GetFullProto()
		proto.Merge(&metadata, fullProto)
	}
	return auth.NewAuthenticationMetadataFromProto(&metadata)
}
