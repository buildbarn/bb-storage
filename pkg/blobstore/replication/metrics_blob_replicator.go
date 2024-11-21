package replication

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
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
		[]string{"operation"})

	blobReplicatorOperationsBlobSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_replicator_operations_blob_size_bytes",
			Help:      "Size of blobs being replicated, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"operation"})

	blobReplicatorOperationsBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_replicator_operations_batch_size",
			Help:      "Number of blobs in batch replication requests.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 17),
		},
		[]string{"operation"})
)

type metricsBlobReplicator struct {
	replicator  BlobReplicator
	clock       clock.Clock
	source      string
	destination string

	singleDurationSeconds    prometheus.ObserverVec
	singleBlobSizeBytes      prometheus.Observer
	compositeDurationSeconds prometheus.ObserverVec
	compositeBlobSizeBytes   prometheus.Observer
	multipleDurationSeconds  prometheus.ObserverVec
	multipleBatchSize        prometheus.Observer
	multipleBlobSizeBytes    prometheus.Observer
}

// NewMetricsBlobReplicator creates a wrapper around BlobReplicator that adds
// Prometheus metrics for monitoring replication operations.
func NewMetricsBlobReplicator(replicator BlobReplicator, clock clock.Clock, storageTypeName string) BlobReplicator {
	replicatorOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(blobReplicatorOperationsDurationSeconds)
		prometheus.MustRegister(blobReplicatorOperationsBlobSizeBytes)
		prometheus.MustRegister(blobReplicatorOperationsBatchSize)
	})

	return &metricsBlobReplicator{
		replicator: replicator,
		clock:      clock,
		singleDurationSeconds: blobReplicatorOperationsDurationSeconds.MustCurryWith(map[string]string{
			"operation": "ReplicateSingle",
			"storage":   storageTypeName,
		}),
		singleBlobSizeBytes: blobReplicatorOperationsBlobSizeBytes.WithLabelValues("ReplicateSingle", storageTypeName),
		compositeDurationSeconds: blobReplicatorOperationsDurationSeconds.MustCurryWith(map[string]string{
			"operation": "ReplicateComposite",
			"storage":   storageTypeName,
		}),
		compositeBlobSizeBytes: blobReplicatorOperationsBlobSizeBytes.WithLabelValues("ReplicateComposite", storageTypeName),
		multipleDurationSeconds: blobReplicatorOperationsDurationSeconds.MustCurryWith(map[string]string{
			"operation": "ReplicateMultiple",
			"storage":   storageTypeName,
		}),
		multipleBlobSizeBytes: blobReplicatorOperationsBlobSizeBytes.WithLabelValues("ReplicateMultiple", storageTypeName),
		multipleBatchSize:     blobReplicatorOperationsBatchSize.WithLabelValues("ReplicateMultiple", storageTypeName),
	}
}

func (r *metricsBlobReplicator) updateDurationSeconds(vec prometheus.ObserverVec, code codes.Code, timeStart time.Time) {
	vec.WithLabelValues(code.String()).Observe(r.clock.Now().Sub(timeStart).Seconds())
}

func (r *metricsBlobReplicator) ReplicateSingle(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	timeStart := r.clock.Now()
	b := buffer.WithErrorHandler(
		r.replicator.ReplicateSingle(ctx, blobDigest),
		&metricsErrorHandler{
			replicator:      r,
			timeStart:       timeStart,
			errorCode:       codes.OK,
			durationSeconds: r.singleDurationSeconds,
		})
	if sizeBytes, err := b.GetSizeBytes(); err == nil {
		r.singleBlobSizeBytes.Observe(float64(sizeBytes))
	}
	return b
}

func (r *metricsBlobReplicator) ReplicateComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	timeStart := r.clock.Now()
	b := buffer.WithErrorHandler(
		r.replicator.ReplicateComposite(ctx, parentDigest, childDigest, slicer),
		&metricsErrorHandler{
			replicator:      r,
			timeStart:       timeStart,
			errorCode:       codes.OK,
			durationSeconds: r.compositeDurationSeconds,
		})
	if sizeBytes, err := b.GetSizeBytes(); err == nil {
		r.compositeBlobSizeBytes.Observe(float64(sizeBytes))
	}
	return b
}

func (r *metricsBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	if digests.Empty() {
		return nil
	}

	timeStart := r.clock.Now()
	r.multipleBatchSize.Observe(float64(digests.Length()))
	// TODO: Add blob size metrics.

	err := r.replicator.ReplicateMultiple(ctx, digests)
	r.updateDurationSeconds(r.multipleDurationSeconds, status.Code(err), timeStart)
	return err
}

type metricsErrorHandler struct {
	replicator      *metricsBlobReplicator
	timeStart       time.Time
	errorCode       codes.Code
	durationSeconds prometheus.ObserverVec
}

func (eh *metricsErrorHandler) OnError(err error) (buffer.Buffer, error) {
	eh.errorCode = status.Code(err)
	return nil, err
}

func (eh *metricsErrorHandler) Done() {
	eh.replicator.updateDurationSeconds(eh.durationSeconds, eh.errorCode, eh.timeStart)
}
