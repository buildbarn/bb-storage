package auth

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type anyAuthorizer struct {
	authorizers []Authorizer
}

// NewAnyAuthorizer creates an Authorizer that forwards calls to a
// series of backends, permitting access to a given instance name if one
// or more backends do so as well.
func NewAnyAuthorizer(authorizers []Authorizer) Authorizer {
	// Keep the implementation of this type simple by limiting to
	// the case where we have multiple authorizers.
	switch len(authorizers) {
	case 0:
		return NewStaticAuthorizer(func(instanceName digest.InstanceName) bool { return false })
	case 1:
		return authorizers[0]
	default:
		return &anyAuthorizer{
			authorizers: authorizers,
		}
	}
}

func (a *anyAuthorizer) Authorize(ctx context.Context, instanceNames []digest.InstanceName) []error {
	// Authorize against the first backend.
	errs := a.authorizers[0].Authorize(ctx, instanceNames)

	// Determine which instance names need to be provided to
	// successive backends.
	var currentInstanceNames []digest.InstanceName
	var currentErrsIndex []int
	for i, err := range errs {
		if status.Code(err) == codes.PermissionDenied {
			currentInstanceNames = append(currentInstanceNames, instanceNames[i])
			currentErrsIndex = append(currentErrsIndex, i)
		}
	}

	// Call into successive backends, each time filtering the list
	// of instance names to request.
	for _, authorizer := range a.authorizers[1:] {
		if len(currentInstanceNames) == 0 {
			break
		}
		nextInstanceNames, nextErrsIndex := currentInstanceNames[:0], currentErrsIndex[:0]
		for i, err := range authorizer.Authorize(ctx, currentInstanceNames) {
			if status.Code(err) == codes.PermissionDenied {
				nextInstanceNames = append(nextInstanceNames, currentInstanceNames[i])
				nextErrsIndex = append(nextErrsIndex, currentErrsIndex[i])
			} else {
				errs[currentErrsIndex[i]] = err
			}
		}
		currentInstanceNames, currentErrsIndex = nextInstanceNames, nextErrsIndex
	}

	return errs
}
