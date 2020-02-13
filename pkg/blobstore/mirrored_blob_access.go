package blobstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opencensus.io/trace"
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
	backendA BlobAccess
	backendB BlobAccess
	round    uint32
}

// NewMirroredBlobAccess creates a BlobAccess that applies operations to
// two storage backends in such a way that they are mirrored. When
// inconsistencies between the two storage backends are detected (i.e.,
// a blob is only present in one of the backends), the blob is
// replicated.
func NewMirroredBlobAccess(backendA BlobAccess, backendB BlobAccess) BlobAccess {
	mirroredBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(mirroredBlobAccessFindMissingSynchronizations)
	})

	return &mirroredBlobAccess{
		backendA: backendA,
		backendB: backendB,
	}
}

func (ba *mirroredBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	ctx, span := trace.StartSpan(ctx, "blobstore.MirroredBlobAccess.Get")
	defer span.End()

	// Alternate requests between storage backends.
	var firstBackend, secondBackend BlobAccess
	var firstBackendName, secondBackendName string
	if atomic.AddUint32(&ba.round, 1)%2 == 1 {
		firstBackend, secondBackend = ba.backendA, ba.backendB
		firstBackendName, secondBackendName = "Backend A", "Backend B"
	} else {
		firstBackend, secondBackend = ba.backendB, ba.backendA
		firstBackendName, secondBackendName = "Backend B", "Backend A"
	}

	return buffer.WithErrorHandler(
		firstBackend.Get(ctx, digest),
		&mirroredErrorHandler{
			firstBackend:      firstBackend,
			firstBackendName:  firstBackendName,
			secondBackend:     secondBackend,
			secondBackendName: secondBackendName,
			context:           ctx,
			digest:            digest,
		})
}

func (ba *mirroredBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	ctx, span := trace.StartSpan(ctx, "blobstore.MirroredBlobAccess.Get")
	defer span.End()

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

func (ba *mirroredBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	// Call FindMissing() on both backends.
	resultsAChan := make(chan findMissingResults, 1)
	go func() {
		resultsAChan <- callFindMissing(ctx, ba.backendA, digests)
	}()
	resultsB := callFindMissing(ctx, ba.backendB, digests)
	resultsA := <-resultsAChan
	if resultsA.err != nil {
		return nil, util.StatusWrap(resultsA.err, "Backend A")
	}
	if resultsB.err != nil {
		return nil, util.StatusWrap(resultsB.err, "Backend B")
	}

	missingFromB := map[string]*util.Digest{}
	for _, digest := range resultsB.missing {
		missingFromB[digest.String()] = digest
	}

	// Synchronize blobs that are missing in A from B.
	var missingFromBoth []*util.Digest
	missingFromA := 0
	for _, digest := range resultsA.missing {
		key := digest.String()
		if _, ok := missingFromB[key]; ok {
			missingFromBoth = append(missingFromBoth, digest)
			delete(missingFromB, key)
		} else {
			if err := ba.backendA.Put(ctx, digest, ba.backendB.Get(ctx, digest)); err != nil {
				return nil, util.StatusWrapf(err, "Failed to synchronize blob %s from backend B to backend A", digest)
			}
			missingFromA++
		}
	}

	// Synchronize blobs that are missing in B from A.
	for _, digest := range missingFromB {
		if err := ba.backendB.Put(ctx, digest, ba.backendA.Get(ctx, digest)); err != nil {
			return nil, util.StatusWrapf(err, "Failed to synchronize blob %s from backend A to backend B", digest)
		}
	}

	mirroredBlobAccessFindMissingSynchronizationsFromAToB.Observe(float64(len(missingFromB)))
	mirroredBlobAccessFindMissingSynchronizationsFromBToA.Observe(float64(missingFromA))

	return missingFromBoth, nil
}

type mirroredErrorHandler struct {
	firstBackend      BlobAccess
	firstBackendName  string
	secondBackend     BlobAccess
	secondBackendName string
	context           context.Context
	digest            *util.Digest
}

func (eh *mirroredErrorHandler) attemptedBothBackends() bool {
	return eh.secondBackend == nil
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
	b1, b2 := eh.secondBackend.Get(eh.context, eh.digest).CloneStream()
	eh.secondBackend = nil
	b1, t := buffer.WithBackgroundTask(b1)
	go func() {
		err := eh.firstBackend.Put(eh.context, eh.digest, b2)
		if err != nil {
			err = util.StatusWrap(err, eh.firstBackendName)
		}
		t.Finish(err)
	}()
	return b1, nil
}

func (eh *mirroredErrorHandler) Done() {}
