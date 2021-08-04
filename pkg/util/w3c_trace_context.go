package util

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
)

// SimpleTextMapCarrier is a simple implementation of
// propagation.TextMapCarrier that is backed by a plain map of strings.
// It is used to preserve W3C Trace Contexts, so that they can be
// embedded into Protobuf messages.
type simpleTextMapCarrier map[string]string

func (c simpleTextMapCarrier) Get(key string) string {
	return c[key]
}

func (c simpleTextMapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func (c simpleTextMapCarrier) Set(key, value string) {
	c[key] = value
}

// W3CTraceContextFromContext extracts the W3C Trace Context headers out
// of the current Context and returns them as a plain map of strings.
// This can, for example, be used to embed the W3C Trace Context into a
// Protobuf message.
//
// This method is useful in case tracing needs to continue across
// processes, even if RPCs flow in mixed directions. For example, to
// make tracing work from clients all the way to workers, the scheduler
// needs to propagate the trace context. Because workers send RPCs to
// the scheduler (not the other way around), the W3C Trace Context
// cannot be forwarded by the OpenTelemetry gRPC middleware. It needs to
// be embedded into the scheduler response explicitly.
func W3CTraceContextFromContext(ctx context.Context) map[string]string {
	c := simpleTextMapCarrier{}
	propagation.TraceContext{}.Inject(ctx, c)
	return c
}

// NewContextWithW3CTraceContext takes a W3C Trace Context that has, for
// example, been embedded into a Protobuf message and returns a new
// Context that uses the W3C Trace Context.
func NewContextWithW3CTraceContext(ctx context.Context, data map[string]string) context.Context {
	return propagation.TraceContext{}.Extract(ctx, simpleTextMapCarrier(data))
}
