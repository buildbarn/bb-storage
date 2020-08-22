package global

import (
	"context"
	"log"
	"net/http"

	// The pprof package does not provide a function for registering
	// its endpoints against an arbitrary mux. Load it to force
	// registration against the default mux, so we can forward
	// traffic to that mux instead.
	_ "net/http/pprof"
	"runtime"
	"time"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/global"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"google.golang.org/grpc/credentials"

	"contrib.go.opencensus.io/exporter/ocagent"
	prometheus_exporter "contrib.go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats/view"
	octrace "go.opencensus.io/trace"

	detectaws "go.opentelemetry.io/contrib/detectors/aws"
	detectgcp "go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/exporters/otlp"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// LifecycleState is returned by ApplyConfiguration. It can be used by
// the caller to report whether the application has started up
// successfully.
type LifecycleState struct {
	diagnosticsHTTPListenAddress string
}

// MarkReadyAndWait can be called to report that the program has started
// successfully. The application should now be reported as being healthy
// and ready, and receive incoming requests if applicable.
func (ls *LifecycleState) MarkReadyAndWait() {
	// Start a diagnostics web server that exposes Prometheus
	// metrics and provides a health check endpoint.
	if ls.diagnosticsHTTPListenAddress == "" {
		select {}
	} else {
		router := mux.NewRouter()
		router.Handle("/metrics", promhttp.Handler())
		router.HandleFunc("/-/healthy", func(http.ResponseWriter, *http.Request) {})
		router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
		log.Fatal(http.ListenAndServe(ls.diagnosticsHTTPListenAddress, router))
	}
}

// ApplyConfiguration applies configuration options to the running
// process. These configuration options are global, in that they apply
// to all Buildbarn binaries, regardless of their purpose.
func ApplyConfiguration(configuration *pb.Configuration) (*LicecycleState, error) {
	if tracingConfiguration := configuration.GetTracing(); tracingConfiguration != nil {
		if tracingConfiguration.OpenTelemetry != nil {
			if err := applyOpenTelemetryConfiguration(tracingConfiguration.OpenTelemetry, tracingConfiguration.SampleProbability); err != nil {
				return nil, err
			}
		}

		if tracingConfiguration.OpenCensus != nil {
			if err := applyOpenCensusConfiguration(tracingConfiguration.OpenCensus, tracingConfiguration.SampleProbability); err != nil {
				return nil, err
			}
		}
	}

	// Enable mutex profiling.
	runtime.SetMutexProfileFraction(int(configuration.GetMutexProfileFraction()))

	// Periodically push metrics to a Prometheus Pushgateway, as
	// opposed to letting the Prometheus server scrape the metrics.
	if pushgateway := configuration.GetPrometheusPushgateway(); pushgateway != nil {
		pusher := push.New(pushgateway.Url, pushgateway.Job)
		pusher.Gatherer(prometheus.DefaultGatherer)
		if basicAuthentication := pushgateway.BasicAuthentication; basicAuthentication != nil {
			pusher.BasicAuth(basicAuthentication.Username, basicAuthentication.Password)
		}
		for key, value := range pushgateway.Grouping {
			pusher.Grouping(key, value)
		}
		pushInterval, err := ptypes.Duration(pushgateway.PushInterval)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse push interval")
		}

		go func() {
			for {
				if err := pusher.Push(); err != nil {
					log.Print("Failed to push metrics to Prometheus Pushgateway: ", err)
				}
				time.Sleep(pushInterval)
			}
		}()
	}

	return &LifecycleState{
		diagnosticsHTTPListenAddress: configuration.GetDiagnosticsHttpListenAddress(),
	}, nil
}

func applyOpenTelemetryConfiguration(config *pb.OpenTelemetryConfiguration, sampleProbability float64) error {
	tlsConfig, err := util.NewTLSConfigFromClientConfiguration(config.Tls)
	if err != nil {
		return util.StatusWrap(err, "Failed to create TLS configuration")
	}

	options := []otlp.ExporterOption{}

	if config.Address != "" {
		options = append(options, otlp.WithAddress(config.Address))
	}

	if tlsConfig != nil {
		options = append(options, otlp.WithTLSCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		options = append(options, otlp.WithInsecure())
	}
	exporter, err := otlp.NewExporter(options...)
	if err != nil {
		return util.StatusWrap(err, "Failed to create OTLP exporter")
	}

	ctx := context.Background()
	var detectors []sdkresource.Detector
	for _, detectorConfig := range config.ResourceDetectors {
		switch detectorConfig {
		case pb.OpenTelemetryConfiguration_FROM_ENVIRONMENT_VARIABLE:
			detectors = append(detectors, &sdkresource.FromEnv{})
		case pb.OpenTelemetryConfiguration_AWS:
			detectors = append(detectors, &detectaws.AWS{})
		case pb.OpenTelemetryConfiguration_GCE:
			detectors = append(detectors, &detectgcp.GCE{})
		case pb.OpenTelemetryConfiguration_GKE:
			detectors = append(detectors, &detectgcp.GKE{})
		}
	}
	resource, err := sdkresource.Detect(ctx, detectors...)
	if err != nil {
		return util.StatusWrap(err, "Failed to create OTLP resource")
	}

	var bopts []sdktrace.BatchSpanProcessorOption
	if config.BlockOnQueueFull {
		bopts = append(bopts, sdktrace.WithBlocking())
	}
	if config.MaxQueueSize > 0 {
		bopts = append(bopts, sdktrace.WithMaxQueueSize(int(config.MaxQueueSize)))
	}
	if config.MaxExportBatchSize > 0 {
		bopts = append(bopts, sdktrace.WithMaxExportBatchSize(int(config.MaxExportBatchSize)))
	}
	if config.BatchTimeout != nil {
		batchTimeout, err := ptypes.Duration(config.BatchTimeout)
		if err != nil {
			return util.StatusWrap(err, "Failed to parse batch timeout")
		}
		bopts = append(bopts, sdktrace.WithBatchTimeout(batchTimeout))
	}

	traceProvider, err := sdktrace.NewProvider(
		sdktrace.WithConfig(sdktrace.Config{DefaultSampler: sdktrace.ProbabilitySampler(sampleProbability)}),
		sdktrace.WithResource(resource),
		sdktrace.WithBatcher(exporter, bopts...),
	)
	if err != nil {
		return util.StatusWrap(err, "Failed to create OTLP provider")
	}

	global.SetTraceProvider(traceProvider)

	return nil
}

func applyOpenCensusConfiguration(config *pb.OpenCensusConfiguration, sampling float64) error {
	tlsConfig, err := util.NewTLSConfigFromClientConfiguration(config.Tls)
	if err != nil {
		return util.StatusWrap(err, "Failed to create TLS configuration")
	}

	options := []ocagent.ExporterOption{}

	if config.Address != "" {
		options = append(options, ocagent.WithAddress(config.Address))
	}
	if config.ServiceName != "" {
		options = append(options, ocagent.WithServiceName(config.ServiceName))
	}

	if tlsConfig != nil {
		options = append(options, ocagent.WithTLSCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		options = append(options, ocagent.WithInsecure())
	}
	exporter, err := ocagent.NewExporter(options...)
	if err != nil {
		return util.StatusWrap(err, "Failed to create OCA exporter")
	}

	view.RegisterExporter(exporter)
	octrace.RegisterExporter(exporter)
	octrace.ApplyConfig(octrace.Config{DefaultSampler: octrace.ProbabilitySampler(sampling)})

	if config.EnablePrometheus {
		pe, err := prometheus_exporter.NewExporter(prometheus_exporter.Options{
			Registry:  prometheus.DefaultRegisterer.(*prometheus.Registry),
			Namespace: "bb_storage",
		})
		if err != nil {
			return util.StatusWrap(err, "Failed to create the Prometheus stats exporter")
		}
		view.RegisterExporter(pe)
	}

	return nil
}
