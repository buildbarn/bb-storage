package replication

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	replicatorOperationsPrometheusMetrics sync.Once

	blobReplicatorOperationsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_replicator_operations_duration_seconds",
			Help:      "Amount of time spent per operation on blob replicator, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"storage_type", "operation", "grpc_code"},
	)

	blobReplicatorOperationsBlobSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_replicator_operations_blob_size_bytes",
			Help:      "Size of the digests of the blobs being replicated, in bytes. Only for the Chunk Store (CS) does this correspond to the actual size of the blobs replicated, and even then compression is not taken into consideration for this.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"storage_type", "operation"},
	)

	blobReplicatorOperationsBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_replicator_operations_batch_size",
			Help:      "Number of blobs in batch replication requests.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 17),
		},
		[]string{"storage_type", "operation"},
	)
)

type metricsBlobReplicator[T any] struct {
	replicator  BlobReplicator[T]
	clock       clock.Clock
	source      string
	destination string

	singleDurationSeconds   prometheus.ObserverVec
	singleBlobSizeBytes     prometheus.Observer
	multipleDurationSeconds prometheus.ObserverVec
	multipleBatchSize       prometheus.Observer
	multipleBlobSizeBytes   prometheus.Observer
}

// NewMetricsBlobReplicator creates a wrapper around BlobReplicator that adds
// Prometheus metrics for monitoring replication operations.
func NewMetricsBlobReplicator[T any](replicator BlobReplicator[T], clock clock.Clock, storageTypeName string) BlobReplicator[T] {
	replicatorOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(blobReplicatorOperationsDurationSeconds)
		prometheus.MustRegister(blobReplicatorOperationsBlobSizeBytes)
		prometheus.MustRegister(blobReplicatorOperationsBatchSize)
	})

	return &metricsBlobReplicator[T]{
		replicator: replicator,
		clock:      clock,
		singleDurationSeconds: blobReplicatorOperationsDurationSeconds.MustCurryWith(map[string]string{
			"storage_type": storageTypeName,
			"operation":    "ReplicateSingle",
		}),
		singleBlobSizeBytes: blobReplicatorOperationsBlobSizeBytes.WithLabelValues(storageTypeName, "ReplicateSingle"),
		multipleDurationSeconds: blobReplicatorOperationsDurationSeconds.MustCurryWith(map[string]string{
			"storage_type": storageTypeName,
			"operation":    "ReplicateMultiple",
		}),
		multipleBlobSizeBytes: blobReplicatorOperationsBlobSizeBytes.WithLabelValues(storageTypeName, "ReplicateMultiple"),
		multipleBatchSize:     blobReplicatorOperationsBatchSize.WithLabelValues(storageTypeName, "ReplicateMultiple"),
	}
}

func (r *metricsBlobReplicator[T]) updateDurationSeconds(vec prometheus.ObserverVec, code codes.Code, timeStart time.Time) {
	vec.WithLabelValues(code.String()).Observe(r.clock.Now().Sub(timeStart).Seconds())
}

func (r *metricsBlobReplicator[T]) ReplicateSingle(ctx context.Context, d digest.Digest) (T, error) {
	timeStart := r.clock.Now()
	r.singleBlobSizeBytes.Observe(float64(d.GetSizeBytes()))
	ret, err := r.replicator.ReplicateSingle(ctx, d)
	r.updateDurationSeconds(r.singleDurationSeconds, status.Code(err), timeStart)
	return ret, err
}

func (r *metricsBlobReplicator[T]) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	if digests.Empty() {
		return nil
	}

	timeStart := r.clock.Now()
	r.multipleBatchSize.Observe(float64(digests.Length()))
	var sumDigestSizeBytes uint64
	for _, d := range digests.Items() {
		sumDigestSizeBytes += uint64(d.GetSizeBytes())
	}
	r.multipleBlobSizeBytes.Observe(float64(sumDigestSizeBytes))
	err := r.replicator.ReplicateMultiple(ctx, digests)
	r.updateDurationSeconds(r.multipleDurationSeconds, status.Code(err), timeStart)
	return err
}
