package mirrored

import (
	"context"
	"sync"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	backendAName = "Backend A"
	backendBName = "Backend B"
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

	mirroredBlobAccessReplicationFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "mirrored_blob_access_replication_failures_total",
			Help:      "Total number of replication failures in resilient mode",
		},
		[]string{"direction"})
	mirroredBlobAccessReplicationFailuresFromAToB = mirroredBlobAccessReplicationFailures.WithLabelValues("FromAToB")
	mirroredBlobAccessReplicationFailuresFromBToA = mirroredBlobAccessReplicationFailures.WithLabelValues("FromBToA")

	resilientBlobBackendFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "resilient_blob_backend_failures_total",
			Help:      "Total number of individual backend failures in resilient mode",
		},
		[]string{"backend", "operation"})
)

type mirroredBlobAccess struct {
	backendA           blobstore.BlobAccess
	backendB           blobstore.BlobAccess
	replicatorAToB     replication.BlobReplicator
	replicatorBToA     replication.BlobReplicator
	round              atomic.Uint32
	requireBothBackends bool
}

// NewMirroredBlobAccess creates a BlobAccess that applies operations to
// two storage backends in such a way that they are mirrored. When
// inconsistencies between the two storage backends are detected (i.e.,
// a blob is only present in one of the backends), the blob is
// replicated.
func NewMirroredBlobAccess(backendA, backendB blobstore.BlobAccess, replicatorAToB, replicatorBToA replication.BlobReplicator) blobstore.BlobAccess {
	return newMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA, true)
}

// NewResilientMirroredBlobAccess creates a BlobAccess that applies operations to
// two storage backends in such a way that they are mirrored, but continues to
// operate when one backend is unavailable, providing better fault tolerance.
// When inconsistencies between the two storage backends are detected, the blob
// is replicated when both backends are available.
func NewResilientMirroredBlobAccess(backendA, backendB blobstore.BlobAccess, replicatorAToB, replicatorBToA replication.BlobReplicator) blobstore.BlobAccess {
	return newMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA, false)
}

func newMirroredBlobAccess(backendA, backendB blobstore.BlobAccess, replicatorAToB, replicatorBToA replication.BlobReplicator, requireBothBackends bool) blobstore.BlobAccess {
	mirroredBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(mirroredBlobAccessFindMissingSynchronizations)
		prometheus.MustRegister(mirroredBlobAccessReplicationFailures)
		prometheus.MustRegister(resilientBlobBackendFailures)
	})

	return &mirroredBlobAccess{
		backendA:           backendA,
		backendB:           backendB,
		replicatorAToB:     replicatorAToB,
		replicatorBToA:     replicatorBToA,
		requireBothBackends: requireBothBackends,
	}
}

func (ba *mirroredBlobAccess) getBlobReplicatorSelector() (blobstore.BlobAccess, replication.BlobReplicatorSelector) {
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

	return firstBackend, func(observedErr error) (replication.BlobReplicator, error) {
		if status.Code(observedErr) != codes.NotFound {
			if replicator != nil {
				if !ba.requireBothBackends {
					replicatorToReturn := replicator
					replicator = nil
					return replicatorToReturn, nil
				}
				return nil, util.StatusWrap(observedErr, firstBackendName)
			}
			return nil, util.StatusWrap(observedErr, secondBackendName)
		}

		// Both storage backends returned NotFound. Return one
		// of the errors in original form.
		if replicator == nil {
			return nil, observedErr
		}

		// Consult the other storage backend. It may still have
		// a copy of the object. Attempt to sync it back to
		// repair this inconsistency.
		replicatorToReturn := replicator
		replicator = nil
		return replicatorToReturn, nil
	}
}

func (ba *mirroredBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	firstBackend, successiveBackends := ba.getBlobReplicatorSelector()
	return replication.GetWithBlobReplicator(ctx, digest, firstBackend, successiveBackends)
}

func (ba *mirroredBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	firstBackend, successiveBackends := ba.getBlobReplicatorSelector()
	return replication.GetFromCompositeWithBlobReplicator(ctx, parentDigest, childDigest, slicer, firstBackend, successiveBackends)
}

func (ba *mirroredBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	b1, b2 := b.CloneStream()
	group, groupCtx := errgroup.WithContext(ctx)

	if ba.requireBothBackends {
		// Strict mode: both backends must succeed
		group.Go(func() error {
			if err := ba.backendA.Put(groupCtx, digest, b1); err != nil {
				return util.StatusWrap(err, backendAName)
			}
			return nil
		})
		group.Go(func() error {
			if err := ba.backendB.Put(groupCtx, digest, b2); err != nil {
				return util.StatusWrap(err, backendBName)
			}
			return nil
		})
		return group.Wait()
	}

	// Resilient mode: continue if one backend fails
	var errA, errB error
	group.Go(func() error {
		if err := ba.backendA.Put(groupCtx, digest, b1); err != nil {
			errA = util.StatusWrap(err, backendAName)
		}
		return nil
	})
	group.Go(func() error {
		if err := ba.backendB.Put(groupCtx, digest, b2); err != nil {
			errB = util.StatusWrap(err, backendBName)
		}
		return nil
	})

	group.Wait()

	if errA != nil && errB != nil {
		return status.Errorf(codes.Internal, "Both backends failed - Backend A: %v, Backend B: %v", errA, errB)
	}

	if errA != nil {
		resilientBlobBackendFailures.WithLabelValues("backend_a", "put").Inc()
	}
	if errB != nil {
		resilientBlobBackendFailures.WithLabelValues("backend_b", "put").Inc()
	}
	return nil
}

func (ba *mirroredBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Call FindMissing() on both backends.
	findMissingGroup, findMissingCtx := errgroup.WithContext(ctx)
	var resultsA, resultsB digest.Set
	var errA, errB error

	if ba.requireBothBackends {
		// Strict mode: both backends must succeed
		findMissingGroup.Go(func() error {
			var err error
			resultsA, err = ba.backendA.FindMissing(findMissingCtx, digests)
			if err != nil {
				return util.StatusWrap(err, backendAName)
			}
			return nil
		})
		findMissingGroup.Go(func() error {
			var err error
			resultsB, err = ba.backendB.FindMissing(findMissingCtx, digests)
			if err != nil {
				return util.StatusWrap(err, backendBName)
			}
			return nil
		})
		if err := findMissingGroup.Wait(); err != nil {
			return digest.EmptySet, err
		}
	} else {
		// Resilient mode: continue if one backend fails
		findMissingGroup.Go(func() error {
			resultsA, errA = ba.backendA.FindMissing(findMissingCtx, digests)
			if errA != nil {
				errA = util.StatusWrap(errA, backendAName)
			}
			return nil
		})
		findMissingGroup.Go(func() error {
			resultsB, errB = ba.backendB.FindMissing(findMissingCtx, digests)
			if errB != nil {
				errB = util.StatusWrap(errB, backendBName)
			}
			return nil
		})

		findMissingGroup.Wait()

		if errA != nil && errB != nil {
			return digest.EmptySet, status.Errorf(codes.Internal, "Both backends failed - Backend A: %v, Backend B: %v", errA, errB)
		}
		if errA != nil {
			resilientBlobBackendFailures.WithLabelValues("backend_a", "find_missing").Inc()
			return resultsB, nil
		}
		if errB != nil {
			resilientBlobBackendFailures.WithLabelValues("backend_b", "find_missing").Inc()
			return resultsA, nil
		}
	}

	// Both backends succeeded, proceed with normal mirroring logic
	// Determine inconsistencies between both backends.
	missingFromA, missingFromBoth, missingFromB := digest.GetDifferenceAndIntersection(resultsA, resultsB)
	mirroredBlobAccessFindMissingSynchronizationsFromAToB.Observe(float64(missingFromB.Length()))
	mirroredBlobAccessFindMissingSynchronizationsFromBToA.Observe(float64(missingFromA.Length()))

	// Exchange objects back and forth.
	replicateGroup, replicateCtx := errgroup.WithContext(ctx)

	if ba.requireBothBackends {
		// Strict mode: replication failures are fatal
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
	} else {
		// Resilient mode: log replication failures but don't fail the operation
		var replicateErrA, replicateErrB error
		replicateGroup.Go(func() error {
			if missingFromB.Length() > 0 {
				if err := ba.replicatorAToB.ReplicateMultiple(replicateCtx, missingFromB); err != nil {
					replicateErrA = err
				}
			}
			return nil
		})
		replicateGroup.Go(func() error {
			if missingFromA.Length() > 0 {
				if err := ba.replicatorBToA.ReplicateMultiple(replicateCtx, missingFromA); err != nil {
					replicateErrB = err
				}
			}
			return nil
		})
		replicateGroup.Wait()

		if replicateErrA != nil {
			mirroredBlobAccessReplicationFailuresFromAToB.Inc()
		}
		if replicateErrB != nil {
			mirroredBlobAccessReplicationFailuresFromBToA.Inc()
		}
	}

	return missingFromBoth, nil
}

func (ba *mirroredBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Alternate requests between storage backends.
	var backend, otherBackend blobstore.BlobAccess
	var backendName, otherBackendName string

	if ba.round.Add(1)%2 == 1 {
		backend, backendName = ba.backendA, backendAName
		otherBackend, otherBackendName = ba.backendB, backendBName
	} else {
		backend, backendName = ba.backendB, backendBName
		otherBackend, otherBackendName = ba.backendA, backendAName
	}

	capabilities, err := backend.GetCapabilities(ctx, instanceName)
	if err != nil {
		if ba.requireBothBackends {
			return nil, util.StatusWrap(err, backendName)
		}

		// Resilient mode: if one backend fails, use the other backend.
		if backendName == backendAName {
			resilientBlobBackendFailures.WithLabelValues("backend_a", "get_capabilities").Inc()
		} else {
			resilientBlobBackendFailures.WithLabelValues("backend_b", "get_capabilities").Inc()
		}
		capabilities, err := otherBackend.GetCapabilities(ctx, instanceName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Both backends failed - %s: %v, %s: %v", backendName, err, otherBackendName, err)
		}
		return capabilities, nil
	}
	return capabilities, nil
}
