package auth

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Authorizer authorizes the requesting user to perform scoped actions
// against particular instance names.
type Authorizer interface {
	// Authorize returns a slice of errors, in the same order as the
	// passed instance names.
	//
	// For each error, a nil value indicates that an instance name was
	// authorized.
	// A non-nil value indicates that the instance name was not authorized,
	// or that an error occurred when authorizing.
	//
	// Note that this function may block, and should not be called while
	// locks are held which may be contended.
	Authorize(ctx context.Context, instanceNames []digest.InstanceName) []error
}

// AuthorizeSingleInstanceName is a convenience function to authorize a
// single instance name with an Authorizer.
func AuthorizeSingleInstanceName(ctx context.Context, authorizer Authorizer, instanceName digest.InstanceName) error {
	return authorizer.Authorize(ctx, []digest.InstanceName{instanceName})[0]
}
