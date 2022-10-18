package http

import (
	"net/http"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	roundTripperPrometheusMetrics sync.Once

	roundTripperRequestsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "http",
			Name:      "round_tripper_requests_duration_seconds",
			Help:      "Amount of time spent per HTTP request, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"name", "code", "method"})
)

// NewMetricsRoundTripper creates an adapter for http.RoundTripper that
// adds basic instrumentation in the form of Prometheus metrics.
func NewMetricsRoundTripper(base http.RoundTripper, name string) http.RoundTripper {
	roundTripperPrometheusMetrics.Do(func() {
		prometheus.MustRegister(roundTripperRequestsDurationSeconds)
	})

	return promhttp.InstrumentRoundTripperDuration(
		roundTripperRequestsDurationSeconds.MustCurryWith(prometheus.Labels{"name": name}),
		base)
}
