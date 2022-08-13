package completenesschecking

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// findMissingQueue is a helper for calling BlobAccess.FindMissing() in
// batches, as opposed to calling it for individual digests.
type findMissingQueue struct {
	context                   context.Context
	instanceName              digest.InstanceName
	contentAddressableStorage blobstore.BlobAccess
	batchSize                 int

	pending digest.SetBuilder
}

// deriveDigest converts a digest embedded into an action result from
// the wire format to an in-memory representation. If that fails, we
// assume that some data corruption has occurred. In that case, we
// should destroy the action result.
func (q *findMissingQueue) deriveDigest(blobDigest *remoteexecution.Digest) (digest.Digest, error) {
	derivedDigest, err := q.instanceName.NewDigestFromProto(blobDigest)
	if err != nil {
		return digest.BadDigest, util.StatusWrapWithCode(err, codes.NotFound, "Action result contained malformed digest")
	}
	return derivedDigest, err
}

// Add a digest to the list of digests that are pending to be checked
// for existence in the Content Addressable Storage.
func (q *findMissingQueue) add(blobDigest *remoteexecution.Digest) error {
	if blobDigest != nil {
		derivedDigest, err := q.deriveDigest(blobDigest)
		if err != nil {
			return err
		}

		if q.pending.Length() >= q.batchSize {
			if err := q.finalize(); err != nil {
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
func (q *findMissingQueue) addDirectory(directory *remoteexecution.Directory) error {
	if directory == nil {
		return nil
	}
	for _, child := range directory.Files {
		if err := q.add(child.Digest); err != nil {
			return err
		}
	}
	return nil
}

// Finalize by checking the last batch of digests for existence.
func (q *findMissingQueue) finalize() error {
	missing, err := q.contentAddressableStorage.FindMissing(q.context, q.pending.Build())
	if err != nil {
		return util.StatusWrap(err, "Failed to determine existence of child objects")
	}
	if digest, ok := missing.First(); ok {
		return status.Errorf(codes.NotFound, "Object %s referenced by the action result is not present in the Content Addressable Storage", digest)
	}
	return nil
}

type completenessCheckingBlobAccess struct {
	blobstore.BlobAccess
	contentAddressableStorage blobstore.BlobAccess
	batchSize                 int
	maximumMessageSizeBytes   int
}

// NewCompletenessCheckingBlobAccess creates a wrapper around
// an Action Cache (AC) that ensures that ActionResult entries are only
// returned in case all objects referenced by the ActionResult are
// present within the Content Addressable Storage (CAS). In case one of
// the referenced objects is absent, the ActionResult entry is treated
// as if non-existent.
//
// The use of this type is required when the underlying Action Cache and
// Content Addressable Storage are two separate data stores that don't
// share a common garbage collection scheme. Tools such as Bazel rely on
// a single call to GetActionResult() to determine whether an action
// needs to be rebuilt. By calling it, Bazel indicates that all
// associated output files must remain present during the build for
// forward progress to be made.
func NewCompletenessCheckingBlobAccess(actionCache, contentAddressableStorage blobstore.BlobAccess, batchSize, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &completenessCheckingBlobAccess{
		BlobAccess:                actionCache,
		contentAddressableStorage: contentAddressableStorage,
		batchSize:                 batchSize,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (ba *completenessCheckingBlobAccess) checkCompleteness(ctx context.Context, instanceName digest.InstanceName, actionResult *remoteexecution.ActionResult) error {
	findMissingQueue := findMissingQueue{
		context:                   ctx,
		instanceName:              instanceName,
		contentAddressableStorage: ba.contentAddressableStorage,
		batchSize:                 ba.batchSize,
		pending:                   digest.NewSetBuilder(),
	}

	// Iterate over all remoteexecution.Digest fields contained
	// within the ActionResult. Check the existence of output
	// directories, even though they are loaded through GetTree()
	// later on. GetTree() may not necessarily cause those objects
	// to be touched.
	for _, outputFile := range actionResult.OutputFiles {
		if err := findMissingQueue.add(outputFile.Digest); err != nil {
			return err
		}
	}
	for _, outputDirectory := range actionResult.OutputDirectories {
		if err := findMissingQueue.add(outputDirectory.TreeDigest); err != nil {
			return err
		}
	}
	if err := findMissingQueue.add(actionResult.StdoutDigest); err != nil {
		return err
	}
	if err := findMissingQueue.add(actionResult.StderrDigest); err != nil {
		return err
	}

	// Iterate over all remoteexecution.Digest fields contained
	// within output directories (remoteexecution.Tree objects)
	// referenced by the ActionResult.
	for _, outputDirectory := range actionResult.OutputDirectories {
		treeDigest, err := findMissingQueue.deriveDigest(outputDirectory.TreeDigest)
		if err != nil {
			return err
		}
		treeMessage, err := ba.contentAddressableStorage.Get(ctx, treeDigest).ToProto(&remoteexecution.Tree{}, ba.maximumMessageSizeBytes)
		if err != nil {
			if status.Code(err) == codes.InvalidArgument {
				return util.StatusWrapfWithCode(err, codes.NotFound, "Failed to fetch output directory %#v", outputDirectory.Path)
			}
			return util.StatusWrapf(err, "Failed to fetch output directory %#v", outputDirectory.Path)
		}
		tree := treeMessage.(*remoteexecution.Tree)
		if err := findMissingQueue.addDirectory(tree.Root); err != nil {
			return err
		}
		for _, child := range tree.Children {
			if err := findMissingQueue.addDirectory(child); err != nil {
				return err
			}
		}
	}
	return findMissingQueue.finalize()
}

func (ba *completenessCheckingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	b1, b2 := ba.BlobAccess.Get(ctx, digest).CloneCopy(ba.maximumMessageSizeBytes)
	actionResult, err := b1.ToProto(&remoteexecution.ActionResult{}, ba.maximumMessageSizeBytes)
	if err != nil {
		b2.Discard()
		return buffer.NewBufferFromError(err)
	}
	if err := ba.checkCompleteness(ctx, digest.GetInstanceName(), actionResult.(*remoteexecution.ActionResult)); err != nil {
		b2.Discard()
		return buffer.NewBufferFromError(err)
	}
	return b2
}
