package auth

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// NewStaticAuthorizer creates a new Authorizer which allows
// all requests to matching instance names, ignoring context.
func NewStaticAuthorizer(matcher digest.InstanceNameMatcher) Authorizer {
	return &staticAuthorizer{matcher: matcher}
}

type staticAuthorizer struct {
	matcher digest.InstanceNameMatcher
}

// Avoid allocating for speed.
var errPermissionDenied = status.Error(codes.PermissionDenied, "Permission denied")

func (a *staticAuthorizer) Authorize(ctx context.Context, instanceNames []digest.InstanceName) []error {
	errs := make([]error, 0, len(instanceNames))
	for _, instanceName := range instanceNames {
		if a.matcher(instanceName) {
			errs = append(errs, nil)
		} else {
			errs = append(errs, errPermissionDenied)
		}
	}
	return errs
}
