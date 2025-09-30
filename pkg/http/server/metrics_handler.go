package server

import (
	"net/http"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	handlerPrometheusMetrics sync.Once

	handlerRequestsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "http",
			Name:      "handler_requests_duration_seconds",
			Help:      "Amount of time spent per HTTP request, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"name", "code", "method"})
)

// NewMetricsHandler creates an adapter for http.Handler that adds basic
// instrumentation in the form of Prometheus metrics.
func NewMetricsHandler(base http.Handler, name string) http.Handler {
	handlerPrometheusMetrics.Do(func() {
		prometheus.MustRegister(handlerRequestsDurationSeconds)
	})

	return promhttp.InstrumentHandlerDuration(
		handlerRequestsDurationSeconds.MustCurryWith(prometheus.Labels{"name": name}),
		base)
}
