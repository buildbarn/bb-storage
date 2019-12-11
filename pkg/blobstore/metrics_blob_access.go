package blobstore

import (
	"context"
	"io"
	"time"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	blobAccessOperationsStartedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_started_total",
			Help:      "Total number of operations started on blob access objects.",
		},
		[]string{"name", "operation"})
	blobAccessOperationsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_duration_seconds",
			Help:      "Amount of time spent per operation on blob access objects, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"name", "operation"})
)

func init() {
	prometheus.MustRegister(blobAccessOperationsStartedTotal)
	prometheus.MustRegister(blobAccessOperationsDurationSeconds)
}

type metricsBlobAccess struct {
	blobAccess                                     BlobAccess
	blobAccessOperationsStartedTotalGet            prometheus.Counter
	blobAccessOperationsDurationSecondsGet         prometheus.Observer
	blobAccessOperationsStartedTotalPut            prometheus.Counter
	blobAccessOperationsDurationSecondsPut         prometheus.Observer
	blobAccessOperationsStartedTotalDelete         prometheus.Counter
	blobAccessOperationsDurationSecondsDelete      prometheus.Observer
	blobAccessOperationsStartedTotalFindMissing    prometheus.Counter
	blobAccessOperationsDurationSecondsFindMissing prometheus.Observer
}

// NewMetricsBlobAccess creates an adapter for BlobAccess that adds
// basic instrumentation in the form of Prometheus metrics.
func NewMetricsBlobAccess(blobAccess BlobAccess, name string) BlobAccess {
	return &metricsBlobAccess{
		blobAccess:                                     blobAccess,
		blobAccessOperationsStartedTotalGet:            blobAccessOperationsStartedTotal.WithLabelValues(name, "Get"),
		blobAccessOperationsDurationSecondsGet:         blobAccessOperationsDurationSeconds.WithLabelValues(name, "Get"),
		blobAccessOperationsStartedTotalPut:            blobAccessOperationsStartedTotal.WithLabelValues(name, "Put"),
		blobAccessOperationsDurationSecondsPut:         blobAccessOperationsDurationSeconds.WithLabelValues(name, "Put"),
		blobAccessOperationsStartedTotalDelete:         blobAccessOperationsStartedTotal.WithLabelValues(name, "Delete"),
		blobAccessOperationsDurationSecondsDelete:      blobAccessOperationsDurationSeconds.WithLabelValues(name, "Delete"),
		blobAccessOperationsStartedTotalFindMissing:    blobAccessOperationsStartedTotal.WithLabelValues(name, "FindMissing"),
		blobAccessOperationsDurationSecondsFindMissing: blobAccessOperationsDurationSeconds.WithLabelValues(name, "FindMissing"),
	}
}

func (ba *metricsBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	ba.blobAccessOperationsStartedTotalGet.Inc()
	timeStart := time.Now()
	length, r, err := ba.blobAccess.Get(ctx, digest)
	ba.blobAccessOperationsDurationSecondsGet.Observe(time.Now().Sub(timeStart).Seconds())
	return length, r, err
}

func (ba *metricsBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	ba.blobAccessOperationsStartedTotalPut.Inc()
	timeStart := time.Now()
	err := ba.blobAccess.Put(ctx, digest, sizeBytes, r)
	ba.blobAccessOperationsDurationSecondsPut.Observe(time.Now().Sub(timeStart).Seconds())
	return err
}

func (ba *metricsBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	ba.blobAccessOperationsStartedTotalDelete.Inc()
	timeStart := time.Now()
	err := ba.blobAccess.Delete(ctx, digest)
	ba.blobAccessOperationsDurationSecondsDelete.Observe(time.Now().Sub(timeStart).Seconds())
	return err
}

func (ba *metricsBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ba.blobAccessOperationsStartedTotalFindMissing.Inc()
	timeStart := time.Now()
	digests, err := ba.blobAccess.FindMissing(ctx, digests)
	ba.blobAccessOperationsDurationSecondsFindMissing.Observe(time.Now().Sub(timeStart).Seconds())
	return digests, err
}
