package util

import (
	"context"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/propagation"
	otel "go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc/status"
)

// RecordError injects an error event and status code into a trace span, if there is one.
func RecordError(ctx context.Context, err error) {
	if err == nil {
		return
	}
	otel.SpanFromContext(ctx).RecordError(ctx, err, otel.WithErrorStatus(codes.Code(status.Code(err))))
}

// Implement go.opentelemetry.io/otel/api/propagation.HTTPSupplier
type mapSupplier map[string]string

func (ms mapSupplier) Get(key string) string {
	return ms[key]
}

func (ms mapSupplier) Set(key string, value string) {
	ms[key] = value
}

// PropagateContextToW3CTraceContext runs HTTP context injection into a map type.
func PropagateContextToW3CTraceContext(ctx context.Context) map[string]string {
	data := map[string]string{}
	propagation.InjectHTTP(ctx, global.Propagators(), mapSupplier(data))
	return data
}

// PropagateW3CTraceContextToContext runs HTTP context extraction from a map type.
func PropagateW3CTraceContextToContext(ctx context.Context, data map[string]string) context.Context {
	return propagation.ExtractHTTP(ctx, global.Propagators(), mapSupplier(data))
}
