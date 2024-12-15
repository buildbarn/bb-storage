package blobstore

import (
	"context"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
	blobAccessOperationsPrometheusMetrics sync.Once

	blobAccessOperationsBlobSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_blob_size_bytes",
			Help:      "Size of blobs being inserted/retrieved, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"storage_type", "backend_type", "operation", "metrics_tag"})
	blobAccessOperationsFindMissingBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_find_missing_batch_size",
			Help:      "Number of digests provided to FindMissing().",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 17),
		},
		[]string{"storage_type", "backend_type", "metrics_tag"})
	blobAccessOperationsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_duration_seconds",
			Help:      "Amount of time spent per operation on blob access objects, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"storage_type", "backend_type", "operation", "metrics_tag", "grpc_code"})
)

type metricsBlobAccess struct {
	blobAccess BlobAccess
	clock      clock.Clock

	getBlobSizeBytes                prometheus.Observer
	getDurationSeconds              prometheus.ObserverVec
	getFromCompositeBlobSizeBytes   prometheus.Observer
	getFromCompositeDurationSeconds prometheus.ObserverVec
	putBlobSizeBytes                prometheus.Observer
	putDurationSeconds              prometheus.ObserverVec
	findMissingBatchSize            prometheus.Observer
	findMissingDurationSeconds      prometheus.ObserverVec
	getCapabilitiesSeconds          prometheus.ObserverVec
}

// NewMetricsBlobAccess creates an adapter for BlobAccess that adds
// basic instrumentation in the form of Prometheus metrics.
func NewMetricsBlobAccess(blobAccess BlobAccess, clock clock.Clock, storageType, backendType, metricsTag string) BlobAccess {
	blobAccessOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(blobAccessOperationsBlobSizeBytes)
		prometheus.MustRegister(blobAccessOperationsFindMissingBatchSize)
		prometheus.MustRegister(blobAccessOperationsDurationSeconds)
	})

	return &metricsBlobAccess{
		blobAccess: blobAccess,
		clock:      clock,

		getBlobSizeBytes:                blobAccessOperationsBlobSizeBytes.WithLabelValues(storageType, backendType, "Get", metricsTag),
		getDurationSeconds:              blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "Get", "metrics_tag": metricsTag}),
		getFromCompositeBlobSizeBytes:   blobAccessOperationsBlobSizeBytes.WithLabelValues(storageType, backendType, "GetFromComposite", metricsTag),
		getFromCompositeDurationSeconds: blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "GetFromComposite", "metrics_tag": metricsTag}),
		putBlobSizeBytes:                blobAccessOperationsBlobSizeBytes.WithLabelValues(storageType, backendType, "Put", metricsTag),
		putDurationSeconds:              blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "Put", "metrics_tag": metricsTag}),
		findMissingBatchSize:            blobAccessOperationsFindMissingBatchSize.WithLabelValues(storageType, backendType, metricsTag),
		findMissingDurationSeconds:      blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "FindMissing", "metrics_tag": metricsTag}),
		getCapabilitiesSeconds:          blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "GetCapabilities", "metrics_tag": metricsTag}),
	}
}

func (ba *metricsBlobAccess) updateDurationSeconds(vec prometheus.ObserverVec, code codes.Code, timeStart time.Time) {
	vec.WithLabelValues(code.String()).Observe(ba.clock.Now().Sub(timeStart).Seconds())
}

func (ba *metricsBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	timeStart := ba.clock.Now()
	b := buffer.WithErrorHandler(
		ba.blobAccess.Get(ctx, digest),
		&metricsErrorHandler{
			blobAccess:      ba,
			timeStart:       timeStart,
			errorCode:       codes.OK,
			durationSeconds: ba.getDurationSeconds,
		})
	if sizeBytes, err := b.GetSizeBytes(); err == nil {
		ba.getBlobSizeBytes.Observe(float64(sizeBytes))
	}
	return b
}

func (ba *metricsBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	timeStart := ba.clock.Now()
	b := buffer.WithErrorHandler(
		ba.blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer),
		&metricsErrorHandler{
			blobAccess:      ba,
			timeStart:       timeStart,
			errorCode:       codes.OK,
			durationSeconds: ba.getFromCompositeDurationSeconds,
		})
	if sizeBytes, err := b.GetSizeBytes(); err == nil {
		ba.getFromCompositeBlobSizeBytes.Observe(float64(sizeBytes))
	}
	return b
}

func (ba *metricsBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	// If the Buffer is in a known error state, return the error
	// here instead of propagating the error to the underlying
	// BlobAccess. Such a Put() call wouldn't have any effect.
	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}
	ba.putBlobSizeBytes.Observe(float64(sizeBytes))

	timeStart := ba.clock.Now()
	err = ba.blobAccess.Put(ctx, digest, b)
	ba.updateDurationSeconds(ba.putDurationSeconds, status.Code(err), timeStart)
	return err
}

func (ba *metricsBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Discard zero-sized FindMissing() requests. These may, for
	// example, be generated by SizeDistinguishingBlobAccess. Such
	// calls would skew the batch size and duration metrics.
	if digests.Empty() {
		return digest.EmptySet, nil
	}

	ba.findMissingBatchSize.Observe(float64(digests.Length()))
	timeStart := ba.clock.Now()
	digests, err := ba.blobAccess.FindMissing(ctx, digests)
	ba.updateDurationSeconds(ba.findMissingDurationSeconds, status.Code(err), timeStart)
	return digests, err
}

func (ba *metricsBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	timeStart := ba.clock.Now()
	capabilities, err := ba.blobAccess.GetCapabilities(ctx, instanceName)
	ba.updateDurationSeconds(ba.getCapabilitiesSeconds, status.Code(err), timeStart)
	return capabilities, err
}

type metricsErrorHandler struct {
	blobAccess      *metricsBlobAccess
	timeStart       time.Time
	errorCode       codes.Code
	durationSeconds prometheus.ObserverVec
}

func (eh *metricsErrorHandler) OnError(err error) (buffer.Buffer, error) {
	eh.errorCode = status.Code(err)
	return nil, err
}

func (eh *metricsErrorHandler) Done() {
	eh.blobAccess.updateDurationSeconds(eh.durationSeconds, eh.errorCode, eh.timeStart)
}
