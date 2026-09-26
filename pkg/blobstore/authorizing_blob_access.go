package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type authorizingBlobAccess[T any] struct {
	BlobAccess[T]

	getAuthorizer         auth.Authorizer
	putAuthorizer         auth.Authorizer
	findMissingAuthorizer auth.Authorizer
}

// NewAuthorizingBlobAccess creates a new BlobAccess which guards blob
// accesses by checks with Authorizers. Calls to GetCapabilities() are
// not checked, for the reason that the exact logic for this differs
// between the Action Cache (AC) and Content Addressable Storage (CAS).
func NewAuthorizingBlobAccess[T any](base BlobAccess[T], getAuthorizer, putAuthorizer, findMissingAuthorizer auth.Authorizer) BlobAccess[T] {
	return &authorizingBlobAccess[T]{
		BlobAccess:            base,
		getAuthorizer:         getAuthorizer,
		putAuthorizer:         putAuthorizer,
		findMissingAuthorizer: findMissingAuthorizer,
	}
}

func (ba *authorizingBlobAccess[T]) Get(ctx context.Context, d digest.Digest) (T, error) {
	var zero T
	if err := auth.AuthorizeSingleInstanceName(ctx, ba.getAuthorizer, d.GetInstanceName()); err != nil {
		return zero, util.StatusWrap(err, "Authorization")
	}
	return ba.BlobAccess.Get(ctx, d)
}

func (ba *authorizingBlobAccess[T]) Put(ctx context.Context, d digest.Digest, value T) error {
	if err := auth.AuthorizeSingleInstanceName(ctx, ba.putAuthorizer, d.GetInstanceName()); err != nil {
		return util.StatusWrap(err, "Authorization")
	}
	return ba.BlobAccess.Put(ctx, d, value)
}

func (ba *authorizingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	instanceNamesSet := make(map[digest.InstanceName]struct{})
	for _, digest := range digests.Items() {
		instanceNamesSet[digest.GetInstanceName()] = struct{}{}
	}
	instanceNames := make([]digest.InstanceName, 0, len(instanceNamesSet))
	for instanceName := range instanceNamesSet {
		instanceNames = append(instanceNames, instanceName)
	}

	errs := ba.findMissingAuthorizer.Authorize(ctx, instanceNames)
	for i, err := range errs {
		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Authorization of instance name %#v", instanceNames[i].String())
		}
	}
	return ba.BlobAccess.FindMissing(ctx, digests)
}
