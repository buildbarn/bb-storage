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

	poolAcquisitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "zstd",
			Name:      "pool_acquisitions_total",
			Help:      "Total number of successful encoder/decoder acquisitions from the pool.",
		},
		[]string{"name", "object_type"})

	poolReleasesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "zstd",
			Name:      "pool_releases_total",
			Help:      "Total number of encoder/decoder releases back to the pool.",
		},
		[]string{"name", "object_type"})

	poolRejectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "zstd",
			Name:      "pool_rejections_total",
			Help:      "Total number of encoder/decoder acquisitions rejected due to context cancellation or pool exhaustion.",
		},
		[]string{"name", "object_type"})

	poolWaitDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "zstd",
			Name:      "pool_wait_duration_seconds",
			Help:      "Time spent waiting to acquire an encoder or decoder from the pool, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16),
		},
		[]string{"name", "object_type"})
)

type metricsPool struct {
	base Pool

	encoderAcquisitions prometheus.Counter
	encoderReleases     prometheus.Counter
	encoderRejections   prometheus.Counter
	encoderWaitDuration prometheus.Observer
	decoderAcquisitions prometheus.Counter
	decoderReleases     prometheus.Counter
	decoderRejections   prometheus.Counter
	decoderWaitDuration prometheus.Observer
	clock               clock.Clock
}

// NewMetricsPool creates a decorator for Pool that exposes Prometheus
// metrics for encoder/decoder acquisition, release, rejection, and wait
// duration.
func NewMetricsPool(base Pool, clock clock.Clock, name string) Pool {
	poolMetricsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(poolAcquisitionsTotal)
		prometheus.MustRegister(poolReleasesTotal)
		prometheus.MustRegister(poolRejectionsTotal)
		prometheus.MustRegister(poolWaitDurationSeconds)
	})

	return &metricsPool{
		base: base,

		encoderAcquisitions: poolAcquisitionsTotal.WithLabelValues(name, "Encoder"),
		encoderReleases:     poolReleasesTotal.WithLabelValues(name, "Encoder"),
		encoderRejections:   poolRejectionsTotal.WithLabelValues(name, "Encoder"),
		encoderWaitDuration: poolWaitDurationSeconds.WithLabelValues(name, "Encoder"),
		decoderAcquisitions: poolAcquisitionsTotal.WithLabelValues(name, "Decoder"),
		decoderReleases:     poolReleasesTotal.WithLabelValues(name, "Decoder"),
		decoderRejections:   poolRejectionsTotal.WithLabelValues(name, "Decoder"),
		decoderWaitDuration: poolWaitDurationSeconds.WithLabelValues(name, "Decoder"),
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
