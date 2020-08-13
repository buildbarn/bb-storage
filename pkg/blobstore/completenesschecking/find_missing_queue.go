package completenesschecking

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FindMissingQueue provides various helper functions for querying a Content
// Addressable Storage for missing blobs in batches of a given size.
type FindMissingQueue struct {
	context                   context.Context
	instanceName              digest.InstanceName
	contentAddressableStorage blobstore.BlobAccess
	batchSize                 int

	pending digest.SetBuilder
}

// NewFindMissingQueue creates a helper for calling BlobAccess.FindMissing() in
// batches, as opposed to calling it for individual digests.
func NewFindMissingQueue(context context.Context, instanceName digest.InstanceName,
	contentAddressableStorage blobstore.BlobAccess,
	batchSize int) FindMissingQueue {
	return FindMissingQueue{
		context:                   context,
		instanceName:              instanceName,
		contentAddressableStorage: contentAddressableStorage,
		batchSize:                 batchSize,
		pending:                   digest.NewSetBuilder(),
	}

}

// DeriveDigest converts a digest embedded into an action result from
// the wire format to an in-memory representation. If that fails, we
// assume that some data corruption has occurred. In that case, we
// should destroy the action result.
func (q *FindMissingQueue) DeriveDigest(blobDigest *remoteexecution.Digest) (digest.Digest, error) {
	derivedDigest, err := q.instanceName.NewDigestFromProto(blobDigest)
	if err != nil {
		return digest.BadDigest, util.StatusWrapWithCode(err, codes.NotFound, "Malformed digest found while checking for result completeness")
	}
	return derivedDigest, err
}

// Add a digest to the list of digests that are pending to be checked
// for existence in the Content Addressable Storage.
func (q *FindMissingQueue) Add(blobDigest *remoteexecution.Digest) error {
	if blobDigest != nil {
		derivedDigest, err := q.DeriveDigest(blobDigest)
		if err != nil {
			return err
		}

		if q.pending.Length() >= q.batchSize {
			if err := q.Finalize(); err != nil {
				return err
			}
			q.pending = digest.NewSetBuilder()
		}
		q.pending.Add(derivedDigest)
	}
	return nil
}

// AddDirectory adds all digests contained with a directory to the list
// of digests pending to be checked for existence.
func (q *FindMissingQueue) AddDirectory(directory *remoteexecution.Directory) error {
	if directory == nil {
		return nil
	}
	for _, child := range directory.Files {
		if err := q.Add(child.Digest); err != nil {
			return err
		}
	}
	return nil
}

// Finalize by checking the last batch of digests for existence.
func (q *FindMissingQueue) Finalize() error {
	missing, err := q.contentAddressableStorage.FindMissing(q.context, q.pending.Build())
	if err != nil {
		return util.StatusWrap(err, "Failed to determine existence of child objects")
	}
	if digest, ok := missing.First(); ok {
		return status.Errorf(codes.NotFound, "Referenced object %s is not present in the Content Addressable Storage", digest)
	}
	return nil
}
