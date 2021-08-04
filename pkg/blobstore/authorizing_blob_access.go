package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type authorizingBlobAccess struct {
	BlobAccess

	getAuthorizer         auth.Authorizer
	putAuthorizer         auth.Authorizer
	findMissingAuthorizer auth.Authorizer
}

// NewAuthorizingBlobAccess creates a new BlobAccess which guards blob accesses by checks with Authorizers.
func NewAuthorizingBlobAccess(base BlobAccess, getAuthorizer, putAuthorizer, findMissingAuthorizer auth.Authorizer) BlobAccess {
	return &authorizingBlobAccess{
		BlobAccess:            base,
		getAuthorizer:         getAuthorizer,
		putAuthorizer:         putAuthorizer,
		findMissingAuthorizer: findMissingAuthorizer,
	}
}

func (ba *authorizingBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	if err := auth.AuthorizeSingleInstanceName(ctx, ba.getAuthorizer, d.GetInstanceName()); err != nil {
		return buffer.NewBufferFromError(util.StatusWrap(err, "Authorization"))
	}
	return ba.BlobAccess.Get(ctx, d)
}

func (ba *authorizingBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	if err := auth.AuthorizeSingleInstanceName(ctx, ba.putAuthorizer, d.GetInstanceName()); err != nil {
		return util.StatusWrap(err, "Authorization")
	}
	return ba.BlobAccess.Put(ctx, d, b)
}

func (ba *authorizingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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
