package opencensus

import (
	"log"

	"contrib.go.opencensus.io/exporter/jaeger"
	prometheus_exporter "contrib.go.opencensus.io/exporter/prometheus"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.opencensus.io/zpages"
)

// Initialize sets up Opentracing with Jaeger and a Prometheus exporter.
func Initialize(configuration *pb.JaegerConfiguration) {
	if configuration != nil {
		pe, err := prometheus_exporter.NewExporter(prometheus_exporter.Options{
			Registry:  prometheus.DefaultRegisterer.(*prometheus.Registry),
			Namespace: "bb_storage",
		})
		if err != nil {
			log.Fatalf("Failed to create the Prometheus stats exporter: %v", err)
		}
		view.RegisterExporter(pe)
		if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
			log.Fatalf("Failed to register ocgrpc server views: %v", err)
		}
		zpages.Handle(nil, "/debug")
		je, err := jaeger.NewExporter(jaeger.Options{
			AgentEndpoint:     configuration.AgentEndpoint,
			CollectorEndpoint: configuration.CollectorEndpoint,
			Process: jaeger.Process{
				ServiceName: configuration.ServiceName,
			},
		})
		if err != nil {
			log.Fatal("Failed to create the Jaeger exporter:", err)
		}
		trace.RegisterExporter(je)
		if configuration.AlwaysSample {
			trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
		}
	}
}
