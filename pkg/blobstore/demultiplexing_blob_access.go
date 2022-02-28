package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// DemultiplexedBlobAccessGetter is a callback that is provided to
// instances of DemultiplexingBlobAccess to resolve instance names to
// backends to which requests need to be forwarded.
//
// For every backend, a name must be provided as well. This name is used
// as part of error messages. The name must be unique, as it is also
// used as a key to identify backends. An InstanceNamePatcher can also
// be returned to adjust the instance name for outgoing requests.
type DemultiplexedBlobAccessGetter func(i digest.InstanceName) (BlobAccess, string, digest.InstanceNamePatcher, error)

type demultiplexingBlobAccess struct {
	getBackend DemultiplexedBlobAccessGetter
}

// NewDemultiplexingBlobAccess creates a BlobAccess that demultiplexes
// requests based on the instance names. This can be used to let
// bb-storage serve as a proxy in front of multiple clusters (e.g., as a
// single on-premise cache).
//
// For every request, calls are made to a DemultiplexedBlobAccessGetter
// callback that provide different backends and mutate instance names on
// outgoing requests.
func NewDemultiplexingBlobAccess(getBackend DemultiplexedBlobAccessGetter) BlobAccess {
	return &demultiplexingBlobAccess{
		getBackend: getBackend,
	}
}

func (ba *demultiplexingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	backend, backendName, patcher, err := ba.getBackend(digest.GetInstanceName())
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.WithErrorHandler(
		backend.Get(ctx, patcher.PatchDigest(digest)),
		backendNamePrefixingErrorHandler{backendName: backendName})
}

func (ba *demultiplexingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	backend, backendName, patcher, err := ba.getBackend(digest.GetInstanceName())
	if err != nil {
		b.Discard()
		return err
	}
	if err := backend.Put(ctx, patcher.PatchDigest(digest), b); err != nil {
		return util.StatusWrapf(err, "Backend %#v", backendName)
	}
	return nil
}

func (ba *demultiplexingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Partition the digest set into one set per backend.
	type partitionInfo struct {
		digests digest.SetBuilder
		backend BlobAccess
		patcher digest.InstanceNamePatcher
	}
	perInstanceNamePartitions := map[digest.InstanceName]*partitionInfo{}
	perBackendPartitions := map[string]*partitionInfo{}
	for _, blobDigest := range digests.Items() {
		instanceName := blobDigest.GetInstanceName()
		partition, ok := perInstanceNamePartitions[instanceName]
		if !ok {
			// This instance name hasn't been observed before.
			backend, backendName, patcher, err := ba.getBackend(instanceName)
			if err != nil {
				return digest.EmptySet, err
			}
			partition, ok = perBackendPartitions[backendName]
			if !ok {
				// This backend hasn't been observed before.
				partition = &partitionInfo{
					digests: digest.NewSetBuilder(),
					backend: backend,
					patcher: patcher,
				}
				perBackendPartitions[backendName] = partition
			}
			perInstanceNamePartitions[instanceName] = partition
		}
		// Change the instance name if requested.
		partition.digests.Add(partition.patcher.PatchDigest(blobDigest))
	}

	// Call FindMissing() on each of the backends and gather the
	// results into a single set.
	//
	// TODO: Would it make sense to perform these calls in parallel?
	// Right now we don't really see calls with multiple instance
	// names, meaning it wouldn't help. This may change in the
	// future.
	allMissing := digest.NewSetBuilder()
	for backendName, partition := range perBackendPartitions {
		partitionMissing, err := partition.backend.FindMissing(ctx, partition.digests.Build())
		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Backend %#v", backendName)
		}
		for _, blobDigest := range partitionMissing.Items() {
			// Undo changes to the instance name.
			allMissing.Add(partition.patcher.UnpatchDigest(blobDigest))
		}
	}
	return allMissing.Build(), nil
}

func (ba *demultiplexingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	backend, backendName, patcher, err := ba.getBackend(instanceName)
	if err != nil {
		return nil, err
	}
	capabilities, err := backend.GetCapabilities(ctx, patcher.PatchInstanceName(instanceName))
	if err != nil {
		return nil, util.StatusWrapf(err, "Backend %#v", backendName)
	}
	return capabilities, err
}

type backendNamePrefixingErrorHandler struct {
	backendName string
}

func (eh backendNamePrefixingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, util.StatusWrapf(err, "Backend %#v", eh.backendName)
}

func (eh backendNamePrefixingErrorHandler) Done() {}
