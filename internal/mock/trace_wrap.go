package mock

import (
	"github.com/golang/mock/gomock"

	"go.opentelemetry.io/otel/trace/embedded"
)

// MockSpan is a wrapper around the gomock stub for trace.Span. We need
// to add embedded.Span to it to satisfy the interface, as it contains
// some private methods.
type MockSpan struct {
	embedded.Span
	*BareMockSpan
}

// NewMockSpan creates a new gomock stub for trace.Span.
func NewMockSpan(ctrl *gomock.Controller) *MockSpan {
	return &MockSpan{
		BareMockSpan: NewBareMockSpan(ctrl),
	}
}

// MockTracer is a wrapper around the gomock stub for trace.Tracer. We
// need to add embedded.Tracer to it to satisfy the interface, as it
// contains some private methods.
type MockTracer struct {
	embedded.Tracer
	*BareMockTracer
}

// NewMockTracer creates a new gomock stub for trace.Tracer.
func NewMockTracer(ctrl *gomock.Controller) *MockTracer {
	return &MockTracer{
		BareMockTracer: NewBareMockTracer(ctrl),
	}
}

// MockTracerProvider is a wrapper around the gomock stub for
// trace.TracerProvider. We need to add embedded.TracerProvider to it to
// satisfy the interface, as it contains some private methods.
type MockTracerProvider struct {
	embedded.TracerProvider
	*BareMockTracerProvider
}

// NewMockTracerProvider creates a new gomock stub for
// trace.TracerProvider.
func NewMockTracerProvider(ctrl *gomock.Controller) *MockTracerProvider {
	return &MockTracerProvider{
		BareMockTracerProvider: NewBareMockTracerProvider(ctrl),
	}
}
