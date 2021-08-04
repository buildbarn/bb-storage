package grpc

import (
	"context"
	"strings"

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
	if len(authenticators) == 1 {
		return authenticators[0]
	}
	return &anyAuthenticator{
		authenticators: authenticators,
	}
}

func (a *anyAuthenticator) Authenticate(ctx context.Context) (context.Context, error) {
	var unauthenticatedErrs []string
	var otherErr error
	for _, authenticator := range a.authenticators {
		newCtx, err := authenticator.Authenticate(ctx)
		if err == nil {
			return newCtx, nil
		}
		if s := status.Convert(err); s.Code() == codes.Unauthenticated {
			unauthenticatedErrs = append(unauthenticatedErrs, s.Message())
		} else if otherErr == nil {
			otherErr = err
		}
	}
	if otherErr != nil {
		return nil, otherErr
	}
	return nil, status.Error(codes.Unauthenticated, strings.Join(unauthenticatedErrs, ", "))
}
