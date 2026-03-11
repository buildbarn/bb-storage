package zstd

import (
	"context"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	poolMetricsPrometheusMetrics sync.Once

	poolOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "zstd",
			Name:      "pool_operations_total",
			Help:      "Total number of encoder/decoder pool operations.",
		},
		[]string{"name", "operation"})

	poolWaitDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "zstd",
			Name:      "pool_wait_duration_seconds",
			Help:      "Time spent waiting to acquire an encoder or decoder from the pool, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16),
		},
		[]string{"name", "operation"})
)

type metricsPool struct {
	base Pool

	encoderAcquisitions    prometheus.Counter
	encoderReleases        prometheus.Counter
	encoderRejections      prometheus.Counter
	encoderWaitDuration    prometheus.Observer
	decoderAcquisitions    prometheus.Counter
	decoderReleases        prometheus.Counter
	decoderRejections      prometheus.Counter
	decoderWaitDuration    prometheus.Observer
	clock                  clock.Clock
}

// NewMetricsPool creates a decorator for Pool that exposes Prometheus
// metrics for encoder/decoder acquisition, release, rejection, and wait
// duration.
func NewMetricsPool(base Pool, clock clock.Clock, name string) Pool {
	poolMetricsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(poolOperationsTotal)
		prometheus.MustRegister(poolWaitDurationSeconds)
	})

	return &metricsPool{
		base: base,

		encoderAcquisitions: poolOperationsTotal.WithLabelValues(name, "EncoderAcquire"),
		encoderReleases:     poolOperationsTotal.WithLabelValues(name, "EncoderRelease"),
		encoderRejections:   poolOperationsTotal.WithLabelValues(name, "EncoderReject"),
		encoderWaitDuration: poolWaitDurationSeconds.WithLabelValues(name, "EncoderAcquire"),
		decoderAcquisitions: poolOperationsTotal.WithLabelValues(name, "DecoderAcquire"),
		decoderReleases:     poolOperationsTotal.WithLabelValues(name, "DecoderRelease"),
		decoderRejections:   poolOperationsTotal.WithLabelValues(name, "DecoderReject"),
		decoderWaitDuration: poolWaitDurationSeconds.WithLabelValues(name, "DecoderAcquire"),
		clock:               clock,
	}
}

func (p *metricsPool) NewEncoder(ctx context.Context, w io.Writer) (Encoder, error) {
	timeStart := p.clock.Now()
	encoder, err := p.base.NewEncoder(ctx, w)
	p.encoderWaitDuration.Observe(p.clock.Now().Sub(timeStart).Seconds())
	if err != nil {
		p.encoderRejections.Inc()
		return nil, err
	}
	p.encoderAcquisitions.Inc()
	return &metricsEncoder{
		Encoder:  encoder,
		releases: p.encoderReleases,
	}, nil
}

func (p *metricsPool) NewDecoder(ctx context.Context, r io.Reader) (Decoder, error) {
	timeStart := p.clock.Now()
	decoder, err := p.base.NewDecoder(ctx, r)
	p.decoderWaitDuration.Observe(p.clock.Now().Sub(timeStart).Seconds())
	if err != nil {
		p.decoderRejections.Inc()
		return nil, err
	}
	p.decoderAcquisitions.Inc()
	return &metricsDecoder{
		Decoder:  decoder,
		releases: p.decoderReleases,
	}, nil
}

type metricsEncoder struct {
	Encoder
	releases prometheus.Counter
}

func (e *metricsEncoder) Close() error {
	err := e.Encoder.Close()
	e.releases.Inc()
	return err
}

type metricsDecoder struct {
	Decoder
	releases prometheus.Counter
}

func (d *metricsDecoder) Close() {
	d.Decoder.Close()
	d.releases.Inc()
}

// Ensure metricsPool implements Pool at compile time.
var _ Pool = (*metricsPool)(nil)
