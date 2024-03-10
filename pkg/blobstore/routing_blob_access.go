package blobstore

import (
	"context"
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Router interface {
	Route(digest.InstanceName) digest.InstanceName
}

func newRouter(config interface{}) Router {
	switch config.(type) {
	}
	return &instanceNameReplacing{newInstanceName: digest.MustNewInstanceName("foo")}
}

type instanceNameReplacing struct {
	newInstanceName digest.InstanceName
}

func NewInstanceNameReplacing(newName string) (Router, error) {
	newInstanceName, err := digest.NewInstanceName(newName)
	if err != nil {
		return nil, fmt.Errorf("failed to create InstanceNameReplacing router with name %q: %w", newName, err)
	}
	return &instanceNameReplacing{
		newInstanceName: newInstanceName,
	}, nil
}

func (r *instanceNameReplacing) Route(in digest.InstanceName) digest.InstanceName {
	return r.newInstanceName
}

type routingBlobAccess struct {
	BlobAccess
	router Router
}

func NewRoutingBlobAccess(blobAccess BlobAccess, router Router) BlobAccess {
	return &routingBlobAccess{
		BlobAccess: blobAccess,
		router:     router,
	}
}

func (ba *routingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return ba.BlobAccess.Get(ctx, reroutedDigest(digest, ba.router))
}

func (ba *routingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	// TODO: Should both digests be rerouted, or just one?
	return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "GetFromComposite not implemented for RoutingBlobAccess"))
}

func (ba *routingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return ba.BlobAccess.Put(ctx, reroutedDigest(digest, ba.router), b)
}

func (ba *routingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Make a set with all instance names rerouted
	// Also make a map of underlying digest -> original digest(s), so that if an
	// underlying digest is missing, we can return the corresponding original
	// digests as also missing.
	reroutedBuilder := digest.NewSetBuilder()
	underlyingToOriginalMap := map[string][]digest.Digest{}
	for _, d := range digests.Items() {
		newDigest := reroutedDigest(d, ba.router)
		reroutedBuilder.Add(newDigest)
		underlyingToOriginalMap[newDigest.GetKey(digest.KeyWithInstance)] = append(
			underlyingToOriginalMap[newDigest.GetKey(digest.KeyWithInstance)],
			d,
		)
	}
	rerouted := reroutedBuilder.Build()

	// Find missing from underlying backend
	underlyingMissing, err := ba.BlobAccess.FindMissing(ctx, rerouted)
	if err != nil {
		return digest.EmptySet, err
	}

	if underlyingMissing.Length() == 0 {
		return underlyingMissing, nil
	}

	originalMissing := digest.NewSetBuilder()
	for _, missing := range underlyingMissing.Items() {
		for _, originalDigest := range underlyingToOriginalMap[missing.GetKey(digest.KeyWithInstance)] {
			originalMissing.Add(originalDigest)
		}
	}
	return originalMissing.Build(), nil
}

func (ba *routingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return ba.BlobAccess.GetCapabilities(ctx, ba.router.Route(instanceName))
}

func reroutedDigest(d digest.Digest, router Router) digest.Digest {
	oldName := d.GetDigestFunction().GetInstanceName()
	return digest.NewInstanceNamePatcher(oldName, router.Route(oldName)).PatchDigest(d)
}
