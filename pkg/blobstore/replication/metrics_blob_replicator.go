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

type metricsBlobReplicator struct {
	replicator  BlobReplicator
	clock       clock.Clock
	source      string
	destination string

	multipleDurationSeconds prometheus.ObserverVec
	multipleBatchSize       prometheus.Observer
}

// NewMetricsBlobReplicator creates a wrapper around BlobReplicator that adds
// Prometheus metrics for monitoring replication operations.
func NewMetricsBlobReplicator(replicator BlobReplicator, clock clock.Clock, storageTypeName string) BlobReplicator {
	replicatorOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(blobReplicatorOperationsDurationSeconds)
		prometheus.MustRegister(blobReplicatorOperationsBatchSize)
	})

	return &metricsBlobReplicator{
		replicator: replicator,
		clock:      clock,
		multipleDurationSeconds: blobReplicatorOperationsDurationSeconds.MustCurryWith(map[string]string{
			"storage_type": storageTypeName,
			"operation":    "ReplicateMultiple",
		}),
		multipleBatchSize: blobReplicatorOperationsBatchSize.WithLabelValues(storageTypeName, "ReplicateMultiple"),
	}
}

func (r *metricsBlobReplicator) updateDurationSeconds(vec prometheus.ObserverVec, code codes.Code, timeStart time.Time) {
	vec.WithLabelValues(code.String()).Observe(r.clock.Now().Sub(timeStart).Seconds())
}

func (r *metricsBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	if digests.Empty() {
		return nil
	}

	timeStart := r.clock.Now()
	r.multipleBatchSize.Observe(float64(digests.Length()))

	err := r.replicator.ReplicateMultiple(ctx, digests)
	r.updateDurationSeconds(r.multipleDurationSeconds, status.Code(err), timeStart)
	return err
}
