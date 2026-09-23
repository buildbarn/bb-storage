package completenesschecking

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// findMissingQueue is a helper for calling BlobAccess.FindMissing() in
// batches, as opposed to calling it for individual digests.
type findMissingQueue struct {
	context              context.Context
	digestFunction       digest.Function
	chunkStorage         blobstore.BlobAccess[*chunk.Chunk]
	chunkListStorage     blobstore.BlobAccess[chunk.List]
	cdcParametersFetcher capabilities.CDCParametersFetcher
	batchSize            int

	pending digest.SetBuilder
}

// deriveDigest converts a digest embedded into an action result from
// the wire format to an in-memory representation. If that fails, we
// assume that some data corruption has occurred. In that case, we
// should destroy the action result.
func (q *findMissingQueue) deriveDigest(blobDigest *remoteexecution.Digest) (digest.Digest, error) {
	derivedDigest, err := q.digestFunction.NewDigestFromProto(blobDigest)
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
			q.pending = digest.NewSetBuilder(q.batchSize)
		}
		q.pending.Add(derivedDigest)
	}
	return nil
}

// Finalize by checking the last batch of digests for existence.
func (q *findMissingQueue) finalize() error {
	params, err := q.cdcParametersFetcher.FetchCDCParameters(q.context, q.digestFunction.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Failed to fetch CDC parameters")
	}
	missing, err := cas.FindMissing(q.context, q.chunkStorage, q.chunkListStorage, params, q.pending.Build())
	if err != nil {
		return util.StatusWrap(err, "Failed to determine existence of child objects")
	}
	if digest, ok := missing.First(); ok {
		return status.Errorf(codes.NotFound, "Object %s referenced by the action result is not present in the Content Addressable Storage", digest)
	}
	return nil
}

type completenessCheckingBlobAccess struct {
	blobstore.BlobAccess[*remoteexecution.ActionResult]
	chunkStorage              blobstore.BlobAccess[*chunk.Chunk]
	chunkListStorage          blobstore.BlobAccess[chunk.List]
	cdcParametersFetcher      capabilities.CDCParametersFetcher
	treeReader                cas.StreamReader
	batchSize                 int
	maximumMessageSizeBytes   int
	maximumTotalTreeSizeBytes int64
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
func NewCompletenessCheckingBlobAccess(actionCache blobstore.BlobAccess[*remoteexecution.ActionResult], chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkListStorage blobstore.BlobAccess[chunk.List], cdcParametersFetcher capabilities.CDCParametersFetcher, treeReader cas.StreamReader, batchSize, maximumMessageSizeBytes int, maximumTotalTreeSizeBytes int64) blobstore.BlobAccess[*remoteexecution.ActionResult] {
	return &completenessCheckingBlobAccess{
		BlobAccess:                actionCache,
		chunkStorage:              chunkStorage,
		chunkListStorage:          chunkListStorage,
		cdcParametersFetcher:      cdcParametersFetcher,
		treeReader:                treeReader,
		batchSize:                 batchSize,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
		maximumTotalTreeSizeBytes: maximumTotalTreeSizeBytes,
	}
}

func (ba *completenessCheckingBlobAccess) checkCompleteness(ctx context.Context, digestFunction digest.Function, actionResult *remoteexecution.ActionResult) error {
	findMissingQueue := findMissingQueue{
		context:              ctx,
		digestFunction:       digestFunction,
		chunkStorage:         ba.chunkStorage,
		chunkListStorage:     ba.chunkListStorage,
		cdcParametersFetcher: ba.cdcParametersFetcher,
		batchSize:            ba.batchSize,
		pending:              digest.NewSetBuilder(ba.batchSize),
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
		if err := findMissingQueue.add(outputDirectory.RootDirectoryDigest); err != nil {
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
	remainingTreeSizeBytes := ba.maximumTotalTreeSizeBytes
	for _, outputDirectory := range actionResult.OutputDirectories {
		treeDigest, err := findMissingQueue.deriveDigest(outputDirectory.TreeDigest)
		if err != nil {
			return err
		}
		sizeBytes := treeDigest.GetSizeBytes()
		if sizeBytes > remainingTreeSizeBytes {
			return status.Errorf(codes.NotFound, "Combined size of all output directories exceeds maximum limit of %d bytes", ba.maximumTotalTreeSizeBytes)
		}
		remainingTreeSizeBytes -= sizeBytes

		r, err := ba.treeReader.ReadStream(ctx, treeDigest)
		if err != nil {
			return err
		}
		if err := util.VisitProtoBytesFields(r, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
			if fieldNumber == blobstore.TreeRootFieldNumber || fieldNumber == blobstore.TreeChildrenFieldNumber {
				directoryData := make([]byte, sizeBytes)
				if _, err := io.ReadFull(fieldReader, directoryData); err != nil {
					return err
				}
				var directory *remoteexecution.Directory = &remoteexecution.Directory{}
				if err := proto.Unmarshal(directoryData, directory); err != nil {
					return err
				}

				// Files are always stored as separate CAS
				// objects. Directories should only be stored
				// as separate CAS objects if we announce them
				// to be present by having the root directory
				// digest set.
				for _, child := range directory.Files {
					if err := findMissingQueue.add(child.Digest); err != nil {
						return err
					}
				}
				if outputDirectory.RootDirectoryDigest != nil {
					for _, child := range directory.Directories {
						if err := findMissingQueue.add(child.Digest); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}); err != nil {
			r.Close()
			return util.StatusWrapf(err, "Output directory %#v", outputDirectory.Path)
		}
		r.Close()
	}
	return findMissingQueue.finalize()
}

func (ba *completenessCheckingBlobAccess) Get(ctx context.Context, digest digest.Digest) (*remoteexecution.ActionResult, error) {
	actionResult, err := ba.BlobAccess.Get(ctx, digest)
	if err != nil {
		return nil, err
	}
	if err := ba.checkCompleteness(ctx, digest.GetDigestFunction(), actionResult); err != nil {
		return nil, err
	}
	return actionResult, nil
}
