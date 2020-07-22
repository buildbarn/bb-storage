package mirrored

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

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
	round          uint32
}

// NewMirroredBlobAccess creates a BlobAccess that applies operations to
// two storage backends in such a way that they are mirrored. When
// inconsistencies between the two storage backends are detected (i.e.,
// a blob is only present in one of the backends), the blob is
// replicated.
func NewMirroredBlobAccess(backendA blobstore.BlobAccess, backendB blobstore.BlobAccess, replicatorAToB replication.BlobReplicator, replicatorBToA replication.BlobReplicator) blobstore.BlobAccess {
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
	if atomic.AddUint32(&ba.round, 1)%2 == 1 {
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
	errAChan := make(chan error, 1)
	go func() {
		errAChan <- ba.backendA.Put(ctx, digest, b1)
	}()
	errB := ba.backendB.Put(ctx, digest, b2)
	if errA := <-errAChan; errA != nil {
		return util.StatusWrap(errA, "Backend A")
	}
	if errB != nil {
		return util.StatusWrap(errB, "Backend B")
	}
	return nil
}

type findMissingResults struct {
	missing digest.Set
	err     error
}

func callFindMissing(ctx context.Context, blobAccess blobstore.BlobAccess, digests digest.Set) findMissingResults {
	missing, err := blobAccess.FindMissing(ctx, digests)
	return findMissingResults{missing: missing, err: err}
}

func (ba *mirroredBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Call FindMissing() on both backends.
	resultsAChan := make(chan findMissingResults, 1)
	go func() {
		resultsAChan <- callFindMissing(ctx, ba.backendA, digests)
	}()
	resultsB := callFindMissing(ctx, ba.backendB, digests)
	resultsA := <-resultsAChan
	if resultsA.err != nil {
		return digest.EmptySet, util.StatusWrap(resultsA.err, "Backend A")
	}
	if resultsB.err != nil {
		return digest.EmptySet, util.StatusWrap(resultsB.err, "Backend B")
	}

	// Determine inconsistencies between both backends.
	missingFromA, missingFromBoth, missingFromB := digest.GetDifferenceAndIntersection(resultsA.missing, resultsB.missing)
	mirroredBlobAccessFindMissingSynchronizationsFromAToB.Observe(float64(missingFromB.Length()))
	mirroredBlobAccessFindMissingSynchronizationsFromBToA.Observe(float64(missingFromA.Length()))

	// Exchange objects back and forth.
	errAToBChan := make(chan error, 1)
	go func() {
		errAToBChan <- ba.replicatorAToB.ReplicateMultiple(ctx, missingFromB)
	}()
	errBToA := ba.replicatorBToA.ReplicateMultiple(ctx, missingFromA)
	errAToB := <-errAToBChan
	if errAToB != nil {
		return digest.EmptySet, util.StatusWrap(errAToB, "Failed to synchronize from backend A to backend B")
	}
	if errBToA != nil {
		return digest.EmptySet, util.StatusWrap(errBToA, "Failed to synchronize from backend B to backend A")
	}
	return missingFromBoth, nil
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
