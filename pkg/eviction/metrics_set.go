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

type metricsSet[T any] struct {
	base Set[T]

	insert prometheus.Counter
	touch  prometheus.Counter
	peek   prometheus.Counter
	remove prometheus.Counter
}

// NewMetricsSet is a decorator for Set that exposes the total number of
// operations performed against the underlying Set through Prometheus.
func NewMetricsSet[T any](base Set[T], name string) Set[T] {
	setOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(setOperationsTotal)
	})

	return &metricsSet[T]{
		base: base,

		insert: setOperationsTotal.WithLabelValues(name, "Insert"),
		touch:  setOperationsTotal.WithLabelValues(name, "Touch"),
		peek:   setOperationsTotal.WithLabelValues(name, "Peek"),
		remove: setOperationsTotal.WithLabelValues(name, "Remove"),
	}
}

func (s *metricsSet[T]) Insert(value T) {
	s.insert.Inc()
	s.base.Insert(value)
}

func (s *metricsSet[T]) Touch(value T) {
	s.touch.Inc()
	s.base.Touch(value)
}

func (s *metricsSet[T]) Peek() T {
	s.peek.Inc()
	return s.base.Peek()
}

func (s *metricsSet[T]) Remove() {
	s.remove.Inc()
	s.base.Remove()
}
