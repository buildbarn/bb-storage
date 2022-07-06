package replication

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type blobToReplicate struct {
	digest       digest.Digest
	expanderFunc func(ctx context.Context, b buffer.Buffer) error
}

// NestedBlobReplicator is a helper type for BlobReplicator that can be
// used to copy nested hierarchies of objects stored in the Content
// Addressable Storage (CAS). In the case of the REv2 protocol, these
// are Action, Directory and Tree messages.
type NestedBlobReplicator struct {
	replicator              BlobReplicator
	digestKeyFormat         digest.KeyFormat
	maximumMessageSizeBytes int

	lock             sync.Mutex
	blobsSeen        map[string]struct{}
	blobsToReplicate []blobToReplicate
	blobsReplicating int
	wakeupChan       chan struct{}
}

// NewNestedBlobReplicator creates a new NestedBlobReplicator that does
// not have any objects to be replicated queued.
func NewNestedBlobReplicator(replicator BlobReplicator, digestKeyFormat digest.KeyFormat, maximumMessageSizeBytes int) *NestedBlobReplicator {
	return &NestedBlobReplicator{
		replicator:              replicator,
		digestKeyFormat:         digestKeyFormat,
		maximumMessageSizeBytes: maximumMessageSizeBytes,

		blobsSeen: map[string]struct{}{},
	}
}

func (nr *NestedBlobReplicator) enqueue(blobDigest digest.Digest, expanderFunc func(ctx context.Context, b buffer.Buffer) error) {
	nr.lock.Lock()
	defer nr.lock.Unlock()

	key := blobDigest.GetKey(nr.digestKeyFormat)
	if _, ok := nr.blobsSeen[key]; !ok {
		nr.blobsSeen[key] = struct{}{}
		nr.blobsToReplicate = append(nr.blobsToReplicate, blobToReplicate{
			digest:       blobDigest,
			expanderFunc: expanderFunc,
		})
		nr.maybeWakeUpLocked()
	}
}

func (nr *NestedBlobReplicator) maybeWakeUpLocked() {
	if nr.wakeupChan != nil {
		close(nr.wakeupChan)
		nr.wakeupChan = nil
	}
}

// EnqueueAction enqueues an REv2 Action to be replicated. The
// referenced input root and Command message will be replicated as well.
func (nr *NestedBlobReplicator) EnqueueAction(actionDigest digest.Digest) {
	instanceName := actionDigest.GetInstanceName()
	nr.enqueue(actionDigest, func(ctx context.Context, b buffer.Buffer) error {
		actionMessage, err := b.ToProto(&remoteexecution.Action{}, nr.maximumMessageSizeBytes)
		if err != nil {
			return err
		}
		action := actionMessage.(*remoteexecution.Action)

		inputRootDigest, err := instanceName.NewDigestFromProto(action.InputRootDigest)
		if err != nil {
			return util.StatusWrap(err, "Invalid input root digest")
		}
		nr.EnqueueDirectory(inputRootDigest)

		commandDigest, err := instanceName.NewDigestFromProto(action.CommandDigest)
		if err != nil {
			return util.StatusWrap(err, "Invalid command digest")
		}
		if err := nr.replicator.ReplicateMultiple(ctx, commandDigest.ToSingletonSet()); err != nil {
			return util.StatusWrap(err, "Failed to replicate command")
		}
		return nil
	})
}

// EnqueueDirectory enqueues an REv2 Directory to be replicated. Any
// referenced file or child Directory message will be replicated as
// well, recursively.
func (nr *NestedBlobReplicator) EnqueueDirectory(directoryDigest digest.Digest) {
	instanceName := directoryDigest.GetInstanceName()
	nr.enqueue(directoryDigest, func(ctx context.Context, b buffer.Buffer) error {
		directoryMessage, err := b.ToProto(&remoteexecution.Directory{}, nr.maximumMessageSizeBytes)
		if err != nil {
			return err
		}
		directory := directoryMessage.(*remoteexecution.Directory)

		for i, childDirectory := range directory.Directories {
			childDigest, err := instanceName.NewDigestFromProto(childDirectory.Digest)
			if err != nil {
				return util.StatusWrapf(err, "Invalid digest for directory at index %d", i)
			}
			nr.EnqueueDirectory(childDigest)
		}

		childFileDigests := digest.NewSetBuilder()
		for i, childFile := range directory.Files {
			childFileDigest, err := instanceName.NewDigestFromProto(childFile.Digest)
			if err != nil {
				return util.StatusWrapf(err, "Invalid digest for file at index %d", i)
			}
			childFileDigests.Add(childFileDigest)
		}
		if err := nr.replicator.ReplicateMultiple(ctx, childFileDigests.Build()); err != nil {
			return util.StatusWrap(err, "Failed to replicate files")
		}
		return nil
	})
}

// EnqueueTree enqueues an REv2 Tree to be replicated. Any referenced
// file will be replicated as well.
func (nr *NestedBlobReplicator) EnqueueTree(treeDigest digest.Digest) {
	instanceName := treeDigest.GetInstanceName()
	nr.enqueue(treeDigest, func(ctx context.Context, b buffer.Buffer) error {
		treeMessage, err := b.ToProto(&remoteexecution.Tree{}, nr.maximumMessageSizeBytes)
		if err != nil {
			return err
		}
		tree := treeMessage.(*remoteexecution.Tree)

		childFileDigests := digest.NewSetBuilder()
		for i, childFile := range tree.Root.GetFiles() {
			childFileDigest, err := instanceName.NewDigestFromProto(childFile.Digest)
			if err != nil {
				return util.StatusWrapf(err, "Invalid digest for file at index %d in root directory", i)
			}
			childFileDigests.Add(childFileDigest)
		}
		for i, childDirectory := range tree.Children {
			for j, childFile := range childDirectory.Files {
				childFileDigest, err := instanceName.NewDigestFromProto(childFile.Digest)
				if err != nil {
					return util.StatusWrapf(err, "Invalid digest for file at index %d in directory at index %d", j, i)
				}
				childFileDigests.Add(childFileDigest)
			}
		}
		if err := nr.replicator.ReplicateMultiple(ctx, childFileDigests.Build()); err != nil {
			return util.StatusWrap(err, "Failed to replicate files")
		}
		return nil
	})
}

// Replicate objects that are enqueued. This method will continue to run
// until all enqueued objects are replicated. It is safe to call this
// method from multiple goroutines, to increase parallelism.
func (nr *NestedBlobReplicator) Replicate(ctx context.Context) error {
	nr.lock.Lock()
	for {
		for len(nr.blobsToReplicate) == 0 {
			if nr.blobsReplicating == 0 {
				// No work available, nor will any work appear.
				nr.lock.Unlock()
				return nil
			}

			// Wait for work to appear.
			if nr.wakeupChan == nil {
				nr.wakeupChan = make(chan struct{})
			}
			wakeupChan := nr.wakeupChan
			nr.lock.Unlock()
			select {
			case <-ctx.Done():
				return util.StatusFromContext(ctx)
			case <-wakeupChan:
			}
			nr.lock.Lock()
		}

		// Dequeue a blob to replicate.
		blobToReplicate := nr.blobsToReplicate[0]
		nr.blobsToReplicate = nr.blobsToReplicate[1:]

		// Replicate a single object.
		nr.blobsReplicating++
		nr.lock.Unlock()
		err := blobToReplicate.expanderFunc(
			ctx,
			nr.replicator.ReplicateSingle(ctx, blobToReplicate.digest),
		)
		nr.lock.Lock()
		nr.blobsReplicating--

		if len(nr.blobsToReplicate) == 0 && nr.blobsReplicating == 0 {
			// No work will appear going forward. Wake up
			// other goroutines that were waiting for us to
			// produce more work.
			nr.maybeWakeUpLocked()
		}

		if err != nil {
			nr.lock.Unlock()
			return util.StatusWrapf(err, "Failed to replicate nested object %#v", blobToReplicate.digest)
		}
	}
}
