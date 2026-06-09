package mirrored

import (
	"context"
	"sync"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
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
		[]string{"direction"},
	)
	mirroredBlobAccessFindMissingSynchronizationsFromAToB = mirroredBlobAccessFindMissingSynchronizations.WithLabelValues("FromAToB")
	mirroredBlobAccessFindMissingSynchronizationsFromBToA = mirroredBlobAccessFindMissingSynchronizations.WithLabelValues("FromBToA")
)

type mirroredBlobAccess[T any] struct {
	backendA       blobstore.BlobAccess[T]
	backendB       blobstore.BlobAccess[T]
	replicatorAToB replication.BlobReplicator
	replicatorBToA replication.BlobReplicator
	round          atomic.Uint32
}

// NewMirroredBlobAccess creates a BlobAccess that applies operations to
// two storage backends in such a way that they are mirrored. When
// inconsistencies between the two storage backends are detected (i.e.,
// a blob is only present in one of the backends), the blob is
// replicated.
func NewMirroredBlobAccess[T any](backendA, backendB blobstore.BlobAccess[T], replicatorAToB, replicatorBToA replication.BlobReplicator) blobstore.BlobAccess[T] {
	mirroredBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(mirroredBlobAccessFindMissingSynchronizations)
	})

	return &mirroredBlobAccess[T]{
		backendA:       backendA,
		backendB:       backendB,
		replicatorAToB: replicatorAToB,
		replicatorBToA: replicatorBToA,
	}
}

func (ba *mirroredBlobAccess[T]) backendAndReplicator() (blobstore.BlobAccess[T], replication.BlobReplicator, string, string) {
	// Alternate requests between storage backends.
	if ba.round.Add(1)%2 == 1 {
		return ba.backendA, ba.replicatorBToA, "Backend A", "Backend B"
	}
	return ba.backendB, ba.replicatorAToB, "Backend B", "Backend A"
}

func (ba *mirroredBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	backend, replicator, name, otherName := ba.backendAndReplicator()
	var zero T
	ret, err := backend.Get(ctx, digest)
	if err == nil {
		return ret, nil
	}
	if status.Code(err) != codes.NotFound {
		return zero, util.StatusWrap(err, name)
	}
	err = replicator.ReplicateMultiple(ctx, digest.ToSingletonSet())
	if err != nil && status.Code(err) != codes.NotFound {
		return zero, util.StatusWrap(err, otherName)
	}

	return backend.Get(ctx, digest)
}

func (ba *mirroredBlobAccess[T]) Put(ctx context.Context, digest digest.Digest, value T) error {
	// Store object in both storage backends.
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		if err := ba.backendA.Put(groupCtx, digest, value); err != nil {
			return util.StatusWrap(err, "Backend A")
		}
		return nil
	})
	group.Go(func() error {
		if err := ba.backendB.Put(groupCtx, digest, value); err != nil {
			return util.StatusWrap(err, "Backend B")
		}
		return nil
	})
	return group.Wait()
}

func (ba *mirroredBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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

func (ba *mirroredBlobAccess[T]) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Alternate requests between storage backends.
	var backend blobstore.BlobAccess[T]
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
