package grpcauth

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type anyAuthenticator struct {
	authenticators []Authenticator
}

// NewAnyAuthenticator wraps a series of Authenticators into a single
// instance. Access is granted only when one or more backing
// Authenticators permit access, similar to Python's any() function.
func NewAnyAuthenticator(authenticators []Authenticator) Authenticator {
	switch len(authenticators) {
	case 0:
		return NewDenyAuthenticator("No authenticators configured")
	case 1:
		return authenticators[0]
	default:
		return &anyAuthenticator{
			authenticators: authenticators,
		}
	}
}

func (a *anyAuthenticator) Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error) {
	var unauthenticatedErrs []error
	var otherErr error
	for _, authenticator := range a.authenticators {
		metadata, err := authenticator.Authenticate(ctx)
		if err == nil {
			return metadata, nil
		}
		if status.Code(err) == codes.Unauthenticated {
			unauthenticatedErrs = append(unauthenticatedErrs, err)
		} else if otherErr == nil {
			otherErr = err
		}
	}
	if otherErr != nil {
		return nil, otherErr
	}
	return nil, util.StatusFromMultiple(unauthenticatedErrs)
}
