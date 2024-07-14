package mock

import (
	"go.opentelemetry.io/otel/trace/embedded"

	"go.uber.org/mock/gomock"
)

// WrappedMockSpan is a wrapper around the gomock stub for trace.Span.
// We need to add embedded.Span to it to satisfy the interface, as it
// contains some private methods.
type WrappedMockSpan struct {
	embedded.Span
	*BareMockSpan
}

// NewMockSpan creates a new gomock stub for trace.Span.
func NewMockSpan(ctrl *gomock.Controller) *WrappedMockSpan {
	return &WrappedMockSpan{
		BareMockSpan: NewBareMockSpan(ctrl),
	}
}

// WrappedMockTracer is a wrapper around the gomock stub for
// trace.Tracer. We need to add embedded.Tracer to it to satisfy the
// interface, as it contains some private methods.
type WrappedMockTracer struct {
	embedded.Tracer
	*BareMockTracer
}

// NewMockTracer creates a new gomock stub for trace.Tracer.
func NewMockTracer(ctrl *gomock.Controller) *WrappedMockTracer {
	return &WrappedMockTracer{
		BareMockTracer: NewBareMockTracer(ctrl),
	}
}

// WrappedMockTracerProvider is a wrapper around the gomock stub for
// trace.TracerProvider. We need to add embedded.TracerProvider to it to
// satisfy the interface, as it contains some private methods.
type WrappedMockTracerProvider struct {
	embedded.TracerProvider
	*BareMockTracerProvider
}

// NewMockTracerProvider creates a new gomock stub for
// trace.TracerProvider.
func NewMockTracerProvider(ctrl *gomock.Controller) *WrappedMockTracerProvider {
	return &WrappedMockTracerProvider{
		BareMockTracerProvider: NewBareMockTracerProvider(ctrl),
	}
}
