package opencensus

import (
	"log"

	"contrib.go.opencensus.io/exporter/jaeger"
	prometheus_exporter "contrib.go.opencensus.io/exporter/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.opencensus.io/zpages"
)

// Initialize will initialize
func Initialize(jaegerAgentEndpointURI, jaegerCollectorEndpointURI, serviceName string, alwaysSample bool) {
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
		AgentEndpoint:     jaegerAgentEndpointURI,
		CollectorEndpoint: jaegerCollectorEndpointURI,
		Process: jaeger.Process{
			ServiceName: serviceName,
		},
	})
	if err != nil {
		log.Fatalf("Failed to create the Jaeger exporter: %v", err)
	}

	trace.RegisterExporter(je)
	if alwaysSample {
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}

}
