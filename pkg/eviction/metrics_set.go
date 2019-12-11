package eviction

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	setOperationsPrometheusMetrics sync.Once

	setOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "eviction",
			Name:      "set_operations_total",
			Help:      "Total number of operations against eviction sets.",
		},
		[]string{"name", "operation"})
)

type metricsSet struct {
	base Set

	insert prometheus.Counter
	touch  prometheus.Counter
	peek   prometheus.Counter
	remove prometheus.Counter
}

// NewMetricsSet is a decorator for Set that exposes the total number of
// operations performed against the underlying Set through Prometheus.
func NewMetricsSet(base Set, name string) Set {
	setOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(setOperationsTotal)
	})

	return &metricsSet{
		base: base,

		insert: setOperationsTotal.WithLabelValues(name, "Insert"),
		touch:  setOperationsTotal.WithLabelValues(name, "Touch"),
		peek:   setOperationsTotal.WithLabelValues(name, "Peek"),
		remove: setOperationsTotal.WithLabelValues(name, "Remove"),
	}
}

func (s *metricsSet) Insert(value string) {
	s.insert.Inc()
	s.base.Insert(value)
}

func (s *metricsSet) Touch(value string) {
	s.touch.Inc()
	s.base.Touch(value)
}

func (s *metricsSet) Peek() string {
	s.peek.Inc()
	return s.base.Peek()
}

func (s *metricsSet) Remove() {
	s.remove.Inc()
	s.base.Remove()
}
