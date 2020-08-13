package completenesschecking

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

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
func NewCompletenessCheckingBlobAccess(actionCache blobstore.BlobAccess, contentAddressableStorage blobstore.BlobAccess, batchSize int, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &completenessCheckingBlobAccess{
		BlobAccess:                actionCache,
		contentAddressableStorage: contentAddressableStorage,
		batchSize:                 batchSize,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (ba *completenessCheckingBlobAccess) checkCompleteness(ctx context.Context, instanceName digest.InstanceName, actionResult *remoteexecution.ActionResult) error {
	findMissingQueue := NewFindMissingQueue(
		ctx,
		instanceName,
		ba.contentAddressableStorage,
		ba.batchSize,
	)

	// Iterate over all remoteexecution.Digest fields contained
	// within the ActionResult. Check the existence of output
	// directories, even though they are loaded through GetTree()
	// later on. GetTree() may not necessarily cause those objects
	// to be touched.
	for _, outputFile := range actionResult.OutputFiles {
		if err := findMissingQueue.Add(outputFile.Digest); err != nil {
			return err
		}
	}
	for _, outputDirectory := range actionResult.OutputDirectories {
		if err := findMissingQueue.Add(outputDirectory.TreeDigest); err != nil {
			return err
		}
	}
	if err := findMissingQueue.Add(actionResult.StdoutDigest); err != nil {
		return err
	}
	if err := findMissingQueue.Add(actionResult.StderrDigest); err != nil {
		return err
	}

	// Iterate over all remoteexecution.Digest fields contained
	// within output directories (remoteexecution.Tree objects)
	// referenced by the ActionResult.
	for _, outputDirectory := range actionResult.OutputDirectories {
		treeDigest, err := findMissingQueue.DeriveDigest(outputDirectory.TreeDigest)
		if err != nil {
			return err
		}
		treeMessage, err := ba.contentAddressableStorage.Get(ctx, treeDigest).ToProto(&remoteexecution.Tree{}, ba.maximumMessageSizeBytes)
		if err != nil {
			return util.StatusWrapf(err, "Failed to fetch output directory %#v", outputDirectory.Path)
		}
		tree := treeMessage.(*remoteexecution.Tree)
		if err := findMissingQueue.AddDirectory(tree.Root); err != nil {
			return err
		}
		for _, child := range tree.Children {
			if err := findMissingQueue.AddDirectory(child); err != nil {
				return err
			}
		}
	}
	return findMissingQueue.Finalize()
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
