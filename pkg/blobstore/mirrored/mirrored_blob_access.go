package mirrored

import (
	"context"
	"sync"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	mirroredBlobAccessPrometheusMetrics sync.Once

	mirroredBlobAccessFindMissingSynchronizations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "mirrored_blob_access_find_missing_synchronizations",
			Help:      "Number of blobs synchronized in FindMissing()",
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1.0, 2.0, 16)...),
		},
		[]string{"direction"})
	mirroredBlobAccessFindMissingSynchronizationsFromAToB = mirroredBlobAccessFindMissingSynchronizations.WithLabelValues("FromAToB")
	mirroredBlobAccessFindMissingSynchronizationsFromBToA = mirroredBlobAccessFindMissingSynchronizations.WithLabelValues("FromBToA")
)

type mirroredBlobAccess struct {
	backendA       blobstore.BlobAccess
	backendB       blobstore.BlobAccess
	replicatorAToB replication.BlobReplicator
	replicatorBToA replication.BlobReplicator
	round          atomic.Uint32
}

// NewMirroredBlobAccess creates a BlobAccess that applies operations to
// two storage backends in such a way that they are mirrored. When
// inconsistencies between the two storage backends are detected (i.e.,
// a blob is only present in one of the backends), the blob is
// replicated.
func NewMirroredBlobAccess(backendA, backendB blobstore.BlobAccess, replicatorAToB, replicatorBToA replication.BlobReplicator) blobstore.BlobAccess {
	mirroredBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(mirroredBlobAccessFindMissingSynchronizations)
	})

	return &mirroredBlobAccess{
		backendA:       backendA,
		backendB:       backendB,
		replicatorAToB: replicatorAToB,
		replicatorBToA: replicatorBToA,
	}
}

func (ba *mirroredBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	// Alternate requests between storage backends.
	var firstBackend blobstore.BlobAccess
	var firstBackendName, secondBackendName string
	var replicator replication.BlobReplicator
	if ba.round.Add(1)%2 == 1 {
		firstBackend = ba.backendA
		firstBackendName, secondBackendName = "Backend A", "Backend B"
		replicator = ba.replicatorBToA
	} else {
		firstBackend = ba.backendB
		firstBackendName, secondBackendName = "Backend B", "Backend A"
		replicator = ba.replicatorAToB
	}

	return buffer.WithErrorHandler(
		firstBackend.Get(ctx, digest),
		&mirroredErrorHandler{
			firstBackendName:  firstBackendName,
			secondBackendName: secondBackendName,
			replicator:        replicator,
			context:           ctx,
			digest:            digest,
		})
}

func (ba *mirroredBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	// Store object in both storage backends.
	b1, b2 := b.CloneStream()
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		if err := ba.backendA.Put(groupCtx, digest, b1); err != nil {
			return util.StatusWrap(err, "Backend A")
		}
		return nil
	})
	group.Go(func() error {
		if err := ba.backendB.Put(groupCtx, digest, b2); err != nil {
			return util.StatusWrap(err, "Backend B")
		}
		return nil
	})
	return group.Wait()
}

func (ba *mirroredBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Call FindMissing() on both backends.
	findMissingGroup, findMissingCtx := errgroup.WithContext(ctx)
	var resultsA, resultsB digest.Set
	findMissingGroup.Go(func() error {
		var err error
		resultsA, err = ba.backendA.FindMissing(findMissingCtx, digests)
		if err != nil {
			return util.StatusWrap(err, "Backend A")
		}
		return nil
	})
	findMissingGroup.Go(func() error {
		var err error
		resultsB, err = ba.backendB.FindMissing(findMissingCtx, digests)
		if err != nil {
			return util.StatusWrap(err, "Backend B")
		}
		return nil
	})
	if err := findMissingGroup.Wait(); err != nil {
		return digest.EmptySet, err
	}

	// Determine inconsistencies between both backends.
	missingFromA, missingFromBoth, missingFromB := digest.GetDifferenceAndIntersection(resultsA, resultsB)
	mirroredBlobAccessFindMissingSynchronizationsFromAToB.Observe(float64(missingFromB.Length()))
	mirroredBlobAccessFindMissingSynchronizationsFromBToA.Observe(float64(missingFromA.Length()))

	// Exchange objects back and forth.
	replicateGroup, replicateCtx := errgroup.WithContext(ctx)
	replicateGroup.Go(func() error {
		if err := ba.replicatorAToB.ReplicateMultiple(replicateCtx, missingFromB); err != nil {
			if status.Code(err) == codes.NotFound {
				return util.StatusWrapWithCode(err, codes.Internal, "Backend A returned inconsistent results while synchronizing")
			}
			return util.StatusWrap(err, "Failed to synchronize from backend A to backend B")
		}
		return nil
	})
	replicateGroup.Go(func() error {
		if err := ba.replicatorBToA.ReplicateMultiple(replicateCtx, missingFromA); err != nil {
			if status.Code(err) == codes.NotFound {
				return util.StatusWrapWithCode(err, codes.Internal, "Backend B returned inconsistent results while synchronizing")
			}
			return util.StatusWrap(err, "Failed to synchronize from backend B to backend A")
		}
		return nil
	})
	if err := replicateGroup.Wait(); err != nil {
		return digest.EmptySet, err
	}
	return missingFromBoth, nil
}

func (ba *mirroredBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Alternate requests between storage backends.
	var backend blobstore.BlobAccess
	var backendName string
	if ba.round.Add(1)%2 == 1 {
		backend = ba.backendA
		backendName = "Backend A"
	} else {
		backend = ba.backendB
		backendName = "Backend B"
	}

	capabilities, err := backend.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrap(err, backendName)
	}
	return capabilities, nil
}

type mirroredErrorHandler struct {
	firstBackendName  string
	secondBackendName string
	replicator        replication.BlobReplicator
	context           context.Context
	digest            digest.Digest
}

func (eh *mirroredErrorHandler) attemptedBothBackends() bool {
	return eh.replicator == nil
}

func (eh *mirroredErrorHandler) OnError(err error) (buffer.Buffer, error) {
	// A fatal error occurred. Prepend the name of the backend that
	// triggered the error.
	if status.Code(err) != codes.NotFound {
		if !eh.attemptedBothBackends() {
			return nil, util.StatusWrap(err, eh.firstBackendName)
		}
		return nil, util.StatusWrap(err, eh.secondBackendName)
	}

	// Both storage backends returned NotFound. Return one of the
	// errors in original form.
	if eh.attemptedBothBackends() {
		return nil, err
	}

	// Consult the other storage backend. It may still have a copy
	// of the object. Attempt to sync it back to repair this
	// inconsistency.
	b := eh.replicator.ReplicateSingle(eh.context, eh.digest)
	eh.replicator = nil
	return b, nil
}

func (eh *mirroredErrorHandler) Done() {}
