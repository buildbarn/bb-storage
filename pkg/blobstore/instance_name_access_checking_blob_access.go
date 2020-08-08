package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type instanceNameAccessCheckingBlobAccess struct {
	BlobAccess
	allowWritesForInstanceName digest.InstanceNameMatcher
}

// NewInstanceNameAccessCheckingBlobAccess is a decorator for BlobAccess
// that only permits write access to storage for certain instance names.
// This can be used to prevent clients from inserting entries into the
// Action Cache (AC) without going through remote execution.
func NewInstanceNameAccessCheckingBlobAccess(base BlobAccess, allowWritesForInstanceName digest.InstanceNameMatcher) BlobAccess {
	return &instanceNameAccessCheckingBlobAccess{
		BlobAccess:                 base,
		allowWritesForInstanceName: allowWritesForInstanceName,
	}
}

func (ba *instanceNameAccessCheckingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	if instanceName := digest.GetInstanceName(); !ba.allowWritesForInstanceName(instanceName) {
		b.Discard()
		return status.Errorf(codes.PermissionDenied, "This service does not permit writes for instance name %#v", instanceName.String())
	}
	return ba.BlobAccess.Put(ctx, digest, b)
}
