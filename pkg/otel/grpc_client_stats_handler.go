package otel

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/semconv/v1.37.0/rpcconv"
	"go.opentelemetry.io/otel/trace"
	grpc_codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type grpcContextKey struct{}

type grpcContext struct {
	inMessages  int64
	outMessages int64
	metricAttrs []attribute.KeyValue
}

type clientStatsHandler struct {
	tracer      trace.Tracer
	propagators propagation.TextMapPropagator

	duration rpcconv.ClientDuration
	inSize   rpcconv.ClientResponseSize
	outSize  rpcconv.ClientRequestSize
	inMsg    rpcconv.ClientResponsesPerRPC
	outMsg   rpcconv.ClientRequestsPerRPC
}

// NewGRPCClientStatsHandler creates a gRPC client stats.Handler that treats
// NotFound responses as non-errors for span status, while still recording
// grpc.status_code.
func NewGRPCClientStatsHandler() stats.Handler {
	tracer := otel.GetTracerProvider().Tracer(
		otelgrpc.ScopeName,
		trace.WithInstrumentationVersion(otelgrpc.Version()),
	)
	meter := otel.GetMeterProvider().Meter(
		otelgrpc.ScopeName,
		metric.WithInstrumentationVersion(otelgrpc.Version()),
		metric.WithSchemaURL(semconv.SchemaURL),
	)

	h := &clientStatsHandler{
		tracer:      tracer,
		propagators: otel.GetTextMapPropagator(),
	}

	var err error
	h.duration, err = rpcconv.NewClientDuration(meter)
	if err != nil {
		otel.Handle(err)
	}
	h.inSize, err = rpcconv.NewClientResponseSize(meter)
	if err != nil {
		otel.Handle(err)
	}
	h.outSize, err = rpcconv.NewClientRequestSize(meter)
	if err != nil {
		otel.Handle(err)
	}
	h.inMsg, err = rpcconv.NewClientResponsesPerRPC(meter)
	if err != nil {
		otel.Handle(err)
	}
	h.outMsg, err = rpcconv.NewClientRequestsPerRPC(meter)
	if err != nil {
		otel.Handle(err)
	}

	return h
}

// TagRPC can attach some information to the given context.
func (h *clientStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	name, attrs := parseFullMethod(info.FullMethodName)
	attrs = append(attrs, semconv.RPCSystemGRPC)

	ctx, _ = h.tracer.Start(
		ctx,
		name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)

	gctx := grpcContext{
		metricAttrs: attrs,
	}

	return inject(context.WithValue(ctx, grpcContextKey{}, &gctx), h.propagators)
}

// HandleRPC processes the RPC stats.
func (h *clientStatsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	h.handleRPC(
		ctx,
		rs,
		h.duration.Inst(),
		h.inSize,
		h.outSize,
		h.inMsg.Inst(),
		h.outMsg.Inst(),
		clientStatus,
	)
}

// TagConn can attach some information to the given context.
func (*clientStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn processes the Conn stats.
func (*clientStatsHandler) HandleConn(context.Context, stats.ConnStats) {
	// no-op
}

type int64Hist interface {
	Record(context.Context, int64, ...attribute.KeyValue)
}

func (h *clientStatsHandler) handleRPC(
	ctx context.Context,
	rs stats.RPCStats,
	duration metric.Float64Histogram,
	inSize, outSize int64Hist,
	inMsg, outMsg metric.Int64Histogram,
	recordStatus func(*status.Status) (codes.Code, string),
) {
	gctx, _ := ctx.Value(grpcContextKey{}).(*grpcContext)
	span := trace.SpanFromContext(ctx)

	switch rs := rs.(type) {
	case *stats.Begin:
	case *stats.InPayload:
		if gctx != nil {
			atomic.AddInt64(&gctx.inMessages, 1)
			inSize.Record(ctx, int64(rs.Length), gctx.metricAttrs...)
		}
	case *stats.OutPayload:
		if gctx != nil {
			atomic.AddInt64(&gctx.outMessages, 1)
			outSize.Record(ctx, int64(rs.Length), gctx.metricAttrs...)
		}
	case *stats.OutTrailer:
	case *stats.OutHeader:
		if span.IsRecording() {
			if p, ok := peer.FromContext(ctx); ok {
				span.SetAttributes(serverAddrAttrs(p.Addr.String())...)
			}
		}
	case *stats.End:
		var rpcStatusAttr attribute.KeyValue

		var s *status.Status
		if rs.Error != nil {
			s, _ = status.FromError(rs.Error)
			rpcStatusAttr = semconv.RPCGRPCStatusCodeKey.Int(int(s.Code()))
		} else {
			rpcStatusAttr = semconv.RPCGRPCStatusCodeKey.Int(int(grpc_codes.OK))
		}
		if span.IsRecording() {
			if s != nil {
				code, msg := recordStatus(s)
				span.SetStatus(code, msg)
			}
			span.SetAttributes(rpcStatusAttr)
			span.End()
		}

		var metricAttrs []attribute.KeyValue
		if gctx != nil {
			metricAttrs = make([]attribute.KeyValue, 0, len(gctx.metricAttrs)+1)
			metricAttrs = append(metricAttrs, gctx.metricAttrs...)
		}
		metricAttrs = append(metricAttrs, rpcStatusAttr)
		recordOpts := []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(metricAttrs...))}

		elapsedTime := float64(rs.EndTime.Sub(rs.BeginTime)) / float64(time.Millisecond)
		duration.Record(ctx, elapsedTime, recordOpts...)
		if gctx != nil {
			inMsg.Record(ctx, atomic.LoadInt64(&gctx.inMessages), recordOpts...)
			outMsg.Record(ctx, atomic.LoadInt64(&gctx.outMessages), recordOpts...)
		}
	default:
		return
	}
}

func clientStatus(grpcStatus *status.Status) (codes.Code, string) {
	if grpcStatus.Code() == grpc_codes.NotFound {
		return codes.Unset, ""
	}
	return codes.Error, grpcStatus.Message()
}

func parseFullMethod(fullMethod string) (string, []attribute.KeyValue) {
	if !strings.HasPrefix(fullMethod, "/") {
		return fullMethod, nil
	}
	name := fullMethod[1:]
	pos := strings.LastIndex(name, "/")
	if pos < 0 {
		return name, nil
	}
	service, method := name[:pos], name[pos+1:]

	var attrs []attribute.KeyValue
	if service != "" {
		attrs = append(attrs, semconv.RPCService(service))
	}
	if method != "" {
		attrs = append(attrs, semconv.RPCMethod(method))
	}
	return name, attrs
}

func serverAddrAttrs(hostport string) []attribute.KeyValue {
	host, portStr, err := net.SplitHostPort(hostport)
	if err != nil {
		return []attribute.KeyValue{semconv.ServerAddress(hostport)}
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return []attribute.KeyValue{semconv.ServerAddress(host)}
	}
	return []attribute.KeyValue{
		semconv.ServerAddress(host),
		semconv.ServerPort(port),
	}
}

type metadataSupplier struct {
	metadata metadata.MD
}

func (s *metadataSupplier) Get(key string) string {
	values := s.metadata.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (s *metadataSupplier) Set(key, value string) {
	s.metadata.Set(key, value)
}

func (s *metadataSupplier) Keys() []string {
	out := make([]string, 0, len(s.metadata))
	for key := range s.metadata {
		out = append(out, key)
	}
	return out
}

func inject(ctx context.Context, propagators propagation.TextMapPropagator) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	propagators.Inject(ctx, &metadataSupplier{
		metadata: md,
	})
	return metadata.NewOutgoingContext(ctx, md)
}
