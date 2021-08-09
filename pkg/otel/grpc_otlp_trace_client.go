package otel

import (
	"context"

	"google.golang.org/grpc"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type grpcOTLPTraceClient struct {
	client coltracepb.TraceServiceClient
}

// NewGRPCOTLPTraceClient creates OTLP trace client that is backed by a
// gRPC connnection. This function is similar to otlptracegrpc's
// NewClient(), except that the gRPC client can be injected. This allows
// the existing gRPC client configuration messages to be reused for
// OpenTelemetry.
func NewGRPCOTLPTraceClient(conn grpc.ClientConnInterface) otlptrace.Client {
	return grpcOTLPTraceClient{
		client: coltracepb.NewTraceServiceClient(conn),
	}
}

func (c grpcOTLPTraceClient) Start(ctx context.Context) error {
	return nil
}

func (c grpcOTLPTraceClient) Stop(ctx context.Context) error {
	return nil
}

func (c grpcOTLPTraceClient) UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
	_, err := c.client.Export(ctx, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: protoSpans,
	})
	return err
}
