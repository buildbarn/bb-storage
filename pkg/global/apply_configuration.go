package global

import (
	"context"
	"io"
	"log"
	"net/http"
	"regexp"
	// The pprof package does not provide a function for registering
	// its endpoints against an arbitrary mux. Load it to force
	// registration against the default mux, so we can forward
	// traffic to that mux instead.
	_ "net/http/pprof"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	bb_otel "github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/buildbarn/bb-storage/pkg/program"
	bb_prometheus "github.com/buildbarn/bb-storage/pkg/prometheus"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/global"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/sercand/kuberesolver/v5"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

// LifecycleState is returned by ApplyConfiguration. It can be used by
// the caller to report whether the application has started up
// successfully.
type LifecycleState struct {
	config                          *pb.DiagnosticsHTTPServerConfiguration
	activeSpansReportingHTTPHandler *bb_otel.ActiveSpansReportingHTTPHandler
}

// MarkReadyAndWait can be called to report that the program has started
// successfully. The application should now be reported as being healthy
// and ready, and receive incoming requests if applicable.
func (ls *LifecycleState) MarkReadyAndWait(group program.Group) {
	// Start a diagnostics web server that exposes Prometheus
	// metrics and provides a health check endpoint.
	if ls.config != nil {
		router := mux.NewRouter()
		router.HandleFunc("/-/healthy", func(http.ResponseWriter, *http.Request) {})
		if ls.config.EnablePrometheus {
			router.Handle("/metrics", promhttp.Handler())
		}
		if ls.config.EnablePprof {
			router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
		}
		if httpHandler := ls.activeSpansReportingHTTPHandler; httpHandler != nil {
			router.Handle("/active_spans", httpHandler)
		}

		bb_http.NewServersFromConfigurationAndServe(
			ls.config.HttpServers,
			bb_http.NewMetricsHandler(router, "Diagnostics"),
			group)
	}
}

// ApplyConfiguration applies configuration options to the running
// process. These configuration options are global, in that they apply
// to all Buildbarn binaries, regardless of their purpose.
func ApplyConfiguration(configuration *pb.Configuration, filesystems []string) (*LifecycleState, bb_grpc.ClientFactory, error) {
	// Set the umask, if requested.
	if setUmaskConfiguration := configuration.GetSetUmask(); setUmaskConfiguration != nil {
		if err := setUmask(setUmaskConfiguration.Umask); err != nil {
			return nil, nil, util.StatusWrap(err, "Failed to set umask")
		}
	}

	// Set resource limits, if provided.
	for name, resourceLimit := range configuration.GetSetResourceLimits() {
		if err := setResourceLimit(name, resourceLimit); err != nil {
			return nil, nil, util.StatusWrapf(err, "Failed to set resource limit %#v", name)
		}
	}

	// Logging.
	logPaths := configuration.GetLogPaths()
	logWriters := append(make([]io.Writer, 0, len(logPaths)+1), os.Stderr)
	for _, logPath := range logPaths {
		w, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			return nil, nil, util.StatusWrapf(err, "Failed to open log path %#v", logPath)
		}
		logWriters = append(logWriters, w)
	}
	log.SetOutput(io.MultiWriter(logWriters...))

	// gRPC resolvers for connecting to Kubernetes service endpoints
	// without using cluster internal DNS.
	for schema, resolverConfiguration := range configuration.GetGrpcKubernetesResolvers() {
		roundTripper, err := bb_http.NewRoundTripperFromConfiguration(resolverConfiguration.ApiServerHttpClient)
		if err != nil {
			return nil, nil, util.StatusWrapf(err, "Failed to create HTTP client for gRPC Kubernetes resolver for schema %#v", schema)
		}
		resolver.Register(
			kuberesolver.NewBuilder(
				newSimpleK8sClient(
					&http.Client{
						Transport: bb_http.NewMetricsRoundTripper(roundTripper, "GRPCKubernetesResolver"),
					},
					resolverConfiguration.ApiServerUrl,
				),
				schema,
			),
		)
	}

	grpcClientDialer := bb_grpc.NewLazyClientDialer(bb_grpc.BaseClientDialer)
	var grpcUnaryInterceptors []grpc.UnaryClientInterceptor
	var grpcStreamInterceptors []grpc.StreamClientInterceptor

	// Optional: gRPC metadata forwarding with reuse.
	if headers := configuration.GetGrpcForwardAndReuseMetadata(); len(headers) > 0 {
		interceptor := bb_grpc.NewMetadataForwardingAndReusingInterceptor(headers)
		grpcUnaryInterceptors = append(grpcUnaryInterceptors, interceptor.InterceptUnaryClient)
		grpcStreamInterceptors = append(grpcStreamInterceptors, interceptor.InterceptStreamClient)
	}

	// Install Prometheus gRPC interceptors, only if the metrics
	// endpoint or Pushgateway is enabled.
	if configuration.GetDiagnosticsHttpServer().GetEnablePrometheus() || configuration.GetPrometheusPushgateway() != nil {
		grpc_prometheus.EnableClientHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets(
				util.DecimalExponentialBuckets(-3, 6, 2)))
		grpcUnaryInterceptors = append(grpcUnaryInterceptors, grpc_prometheus.UnaryClientInterceptor)
		grpcStreamInterceptors = append(grpcStreamInterceptors, grpc_prometheus.StreamClientInterceptor)

		// If there are file systems to monitor, start the goroutine to sample the file systems statistics periodically.
		if len(filesystems) != 0 {
			go func() {
				fsStats := prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Namespace: "buildbarn",
						Subsystem: "system",
						Name:      "fs_stats",
						Help:      "File system usage statistics",
					},
					[]string{"fs_name", "resource"})
				prometheus.MustRegister(fsStats)
				for {
					for _, fs := range filesystems {
						var sbuf syscall.Statfs_t
						err := syscall.Statfs(fs, &sbuf)
						if err != nil {
							log.Print("Failed to get filesystem stats: ", err)
							continue
						}
						// Calculate % blocks used.
						bu := 100.0 * float64(sbuf.Blocks - sbuf.Bavail) / float64(sbuf.Blocks)
						// Calculate % inodes used.
						iu := 100.0 * float64(sbuf.Files - sbuf.Ffree) / float64(sbuf.Files)
						fsStats.WithLabelValues(fs, "blocks").Set(bu)
						fsStats.WithLabelValues(fs, "inodes").Set(iu)
					}
					time.Sleep(30*time.Second)
				}
			}()
		}
	}

	// Perform tracing using OpenTelemetry.
	var activeSpansReportingHTTPHandler *bb_otel.ActiveSpansReportingHTTPHandler
	if tracingConfiguration, enableActiveSpans := configuration.GetTracing(), configuration.GetDiagnosticsHttpServer().GetEnableActiveSpans(); tracingConfiguration != nil || enableActiveSpans {
		tracerProvider := trace.NewNoopTracerProvider()
		if tracingConfiguration != nil {
			// Special gRPC client factory that doesn't have tracing
			// enabled. This must be used by the OTLP span exporter
			// to prevent infinitely recursive traces.
			nonTracingGRPCClientFactory := bb_grpc.NewDeduplicatingClientFactory(
				bb_grpc.NewBaseClientFactory(
					grpcClientDialer,
					grpcUnaryInterceptors,
					grpcStreamInterceptors))

			var tracerProviderOptions []sdktrace.TracerProviderOption
			for _, backend := range tracingConfiguration.Backends {
				// Construct a SpanExporter.
				var spanExporter sdktrace.SpanExporter
				switch spanExporterConfiguration := backend.SpanExporter.(type) {
				case *pb.TracingConfiguration_Backend_JaegerCollectorSpanExporter_:
					// Convert Jaeger collector configuration
					// message to a list of options.
					jaegerConfiguration := spanExporterConfiguration.JaegerCollectorSpanExporter
					var collectorEndpointOptions []jaeger.CollectorEndpointOption
					if endpoint := jaegerConfiguration.Endpoint; endpoint != "" {
						collectorEndpointOptions = append(collectorEndpointOptions, jaeger.WithEndpoint(endpoint))
					}
					roundTripper, err := bb_http.NewRoundTripperFromConfiguration(jaegerConfiguration.HttpClient)
					if err != nil {
						return nil, nil, util.StatusWrap(err, "Failed to create Jaeger collector HTTP client")
					}
					collectorEndpointOptions = append(collectorEndpointOptions, jaeger.WithHTTPClient(&http.Client{
						Transport: bb_http.NewMetricsRoundTripper(roundTripper, "Jaeger"),
					}))
					if password := jaegerConfiguration.Password; password != "" {
						collectorEndpointOptions = append(collectorEndpointOptions, jaeger.WithPassword(password))
					}
					if username := jaegerConfiguration.Password; username != "" {
						collectorEndpointOptions = append(collectorEndpointOptions, jaeger.WithUsername(username))
					}

					// Construct a Jaeger span exporter.
					exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(collectorEndpointOptions...))
					if err != nil {
						return nil, nil, util.StatusWrap(err, "Failed to create Jaeger collector span exporter")
					}
					spanExporter = exporter
				case *pb.TracingConfiguration_Backend_OtlpSpanExporter:
					client, err := nonTracingGRPCClientFactory.NewClientFromConfiguration(spanExporterConfiguration.OtlpSpanExporter)
					if err != nil {
						return nil, nil, util.StatusWrap(err, "Failed to create OTLP gRPC client")
					}
					spanExporter, err = otlptrace.New(context.Background(), bb_otel.NewGRPCOTLPTraceClient(client))
					if err != nil {
						return nil, nil, util.StatusWrap(err, "Failed to create OTLP span exporter")
					}
				default:
					return nil, nil, status.Error(codes.InvalidArgument, "Tracing backend does not contain a valid span exporter")
				}

				// Wrap it in a SpanProcessor.
				var spanProcessor sdktrace.SpanProcessor
				switch spanProcessorConfiguration := backend.SpanProcessor.(type) {
				case *pb.TracingConfiguration_Backend_SimpleSpanProcessor:
					spanProcessor = sdktrace.NewSimpleSpanProcessor(spanExporter)
				case *pb.TracingConfiguration_Backend_BatchSpanProcessor_:
					var batchSpanProcessorOptions []sdktrace.BatchSpanProcessorOption
					if d := spanProcessorConfiguration.BatchSpanProcessor.BatchTimeout; d != nil {
						if err := d.CheckValid(); err != nil {
							return nil, nil, util.StatusWrap(err, "Invalid batch span processor batch timeout")
						}
						batchSpanProcessorOptions = append(batchSpanProcessorOptions, sdktrace.WithBatchTimeout(d.AsDuration()))
					}
					if spanProcessorConfiguration.BatchSpanProcessor.Blocking {
						batchSpanProcessorOptions = append(batchSpanProcessorOptions, sdktrace.WithBlocking())
					}
					if d := spanProcessorConfiguration.BatchSpanProcessor.ExportTimeout; d != nil {
						if err := d.CheckValid(); err != nil {
							return nil, nil, util.StatusWrap(err, "Invalid batch span processor export timeout")
						}
						batchSpanProcessorOptions = append(batchSpanProcessorOptions, sdktrace.WithExportTimeout(d.AsDuration()))
					}
					if size := spanProcessorConfiguration.BatchSpanProcessor.MaxExportBatchSize; size != 0 {
						batchSpanProcessorOptions = append(batchSpanProcessorOptions, sdktrace.WithMaxExportBatchSize(int(size)))
					}
					if size := spanProcessorConfiguration.BatchSpanProcessor.MaxQueueSize; size != 0 {
						batchSpanProcessorOptions = append(batchSpanProcessorOptions, sdktrace.WithMaxQueueSize(int(size)))
					}
					spanProcessor = sdktrace.NewBatchSpanProcessor(spanExporter, batchSpanProcessorOptions...)
				default:
					return nil, nil, status.Error(codes.InvalidArgument, "Tracing backend does not contain a valid span processor")
				}
				tracerProviderOptions = append(tracerProviderOptions, sdktrace.WithSpanProcessor(spanProcessor))
			}

			// Set resource attributes, so that this process can be
			// identified uniquely.
			resourceAttributes, err := bb_otel.NewKeyValueListFromProto(tracingConfiguration.ResourceAttributes, "")
			if err != nil {
				return nil, nil, util.StatusWrap(err, "Failed to create resource attributes")
			}
			tracerProviderOptions = append(
				tracerProviderOptions,
				sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, resourceAttributes...)))

			// Create a Sampler, acting as a policy for when to sample.
			sampler, err := newSamplerFromConfiguration(tracingConfiguration.Sampler)
			if err != nil {
				return nil, nil, util.StatusWrap(err, "Failed to create sampler")
			}
			tracerProviderOptions = append(tracerProviderOptions, sdktrace.WithSampler(sampler))
			tracerProvider = sdktrace.NewTracerProvider(tracerProviderOptions...)
		}

		if enableActiveSpans {
			activeSpansReportingHTTPHandler = bb_otel.NewActiveSpansReportingHTTPHandler(clock.SystemClock)
			tracerProvider = activeSpansReportingHTTPHandler.NewTracerProvider(tracerProvider)
		}

		otel.SetTracerProvider(tracerProvider)

		// Construct a propagator which supports both the context and Zipkin B3 propagation standards.
		propagator := propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader)),
		)
		otel.SetTextMapPropagator(propagator)

		grpcUnaryInterceptors = append(grpcUnaryInterceptors, otelgrpc.UnaryClientInterceptor())
		grpcStreamInterceptors = append(grpcStreamInterceptors, otelgrpc.StreamClientInterceptor())
	}

	// Enable mutex profiling.
	runtime.SetMutexProfileFraction(int(configuration.GetMutexProfileFraction()))

	// Periodically push metrics to a Prometheus Pushgateway, as
	// opposed to letting the Prometheus server scrape the metrics.
	if pushgateway := configuration.GetPrometheusPushgateway(); pushgateway != nil {
		gatherer := prometheus.DefaultGatherer
		if len(pushgateway.AdditionalScrapeTargets) > 0 {
			// Set up scraping of additional targets, such
			// as Prometheus Node Exporter.
			allGatherers := make(prometheus.Gatherers, 0, len(pushgateway.AdditionalScrapeTargets)+1)
			for _, scrapeTarget := range pushgateway.AdditionalScrapeTargets {
				roundTripper, err := bb_http.NewRoundTripperFromConfiguration(scrapeTarget.HttpClient)
				if err != nil {
					return nil, nil, util.StatusWrapf(err, "Failed to create HTTP client for additional scrape target %#v", scrapeTarget.Url)
				}
				metricNamePattern, err := regexp.Compile(scrapeTarget.MetricNamePattern)
				if err != nil {
					return nil, nil, util.StatusWrapfWithCode(err, codes.InvalidArgument, "Invalid metric name pattern %#v for additional scrape target %#v", scrapeTarget.MetricNamePattern, scrapeTarget.Url)
				}
				allGatherers = append(
					allGatherers,
					bb_prometheus.NewNameFilteringGatherer(
						bb_prometheus.NewHTTPGatherer(
							&http.Client{Transport: roundTripper},
							scrapeTarget.Url),
						metricNamePattern))
			}
			gatherer = append(allGatherers, gatherer)
		}

		pusher := push.New(pushgateway.Url, pushgateway.Job)
		pusher.Gatherer(gatherer)
		for key, value := range pushgateway.Grouping {
			pusher.Grouping(key, value)
		}
		roundTripper, err := bb_http.NewRoundTripperFromConfiguration(pushgateway.HttpClient)
		if err != nil {
			return nil, nil, util.StatusWrap(err, "Failed to create Prometheus Pushgateway HTTP client")
		}
		pusher.Client(&http.Client{
			Transport: bb_http.NewMetricsRoundTripper(roundTripper, "Pushgateway"),
		})

		pushInterval := pushgateway.PushInterval
		if err := pushInterval.CheckValid(); err != nil {
			return nil, nil, util.StatusWrap(err, "Failed to parse push interval")
		}
		pushIntervalDuration := pushInterval.AsDuration()

		pushTimeout := pushgateway.PushTimeout
		if err := pushTimeout.CheckValid(); err != nil {
			return nil, nil, util.StatusWrap(err, "Failed to parse push timeout")
		}
		pushTimeoutDuration := pushTimeout.AsDuration()

		// TODO: Run this as part of the program.Group, so that
		// it gets cleaned up upon shutdown.
		go func() {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), pushTimeoutDuration)
				err := pusher.PushContext(ctx)
				cancel()
				if err != nil {
					log.Print("Failed to push metrics to Prometheus Pushgateway: ", err)
				}
				time.Sleep(pushIntervalDuration)
			}
		}()
	}

	return &LifecycleState{
			config:                          configuration.GetDiagnosticsHttpServer(),
			activeSpansReportingHTTPHandler: activeSpansReportingHTTPHandler,
		},
		bb_grpc.NewDeduplicatingClientFactory(
			bb_grpc.NewBaseClientFactory(
				grpcClientDialer,
				grpcUnaryInterceptors,
				grpcStreamInterceptors)),
		nil
}

// NewSamplerFromConfiguration creates a OpenTelemetry Sampler based on
// a configuration file.
func newSamplerFromConfiguration(configuration *pb.TracingConfiguration_Sampler) (sdktrace.Sampler, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No configuration provided")
	}
	switch policy := configuration.Policy.(type) {
	case *pb.TracingConfiguration_Sampler_Always:
		return sdktrace.AlwaysSample(), nil
	case *pb.TracingConfiguration_Sampler_Never:
		return sdktrace.NeverSample(), nil
	case *pb.TracingConfiguration_Sampler_ParentBased_:
		noParent, err := newSamplerFromConfiguration(policy.ParentBased.NoParent)
		if err != nil {
			return nil, util.StatusWrap(err, "No parent")
		}
		localParentNotSampled, err := newSamplerFromConfiguration(policy.ParentBased.LocalParentNotSampled)
		if err != nil {
			return nil, util.StatusWrap(err, "Local parent not sampled")
		}
		localParentSampled, err := newSamplerFromConfiguration(policy.ParentBased.LocalParentSampled)
		if err != nil {
			return nil, util.StatusWrap(err, "Local parent sampled")
		}
		remoteParentNotSampled, err := newSamplerFromConfiguration(policy.ParentBased.RemoteParentNotSampled)
		if err != nil {
			return nil, util.StatusWrap(err, "Remote parent not sampled")
		}
		remoteParentSampled, err := newSamplerFromConfiguration(policy.ParentBased.RemoteParentSampled)
		if err != nil {
			return nil, util.StatusWrap(err, "Remote parent sampled")
		}
		return sdktrace.ParentBased(
			noParent,
			sdktrace.WithLocalParentNotSampled(localParentNotSampled),
			sdktrace.WithLocalParentSampled(localParentSampled),
			sdktrace.WithRemoteParentNotSampled(remoteParentNotSampled),
			sdktrace.WithRemoteParentSampled(remoteParentSampled)), nil
	case *pb.TracingConfiguration_Sampler_TraceIdRatioBased:
		return sdktrace.TraceIDRatioBased(policy.TraceIdRatioBased), nil
	case *pb.TracingConfiguration_Sampler_MaximumRate_:
		epochDuration := policy.MaximumRate.EpochDuration
		if err := epochDuration.CheckValid(); err != nil {
			return nil, util.StatusWrap(err, "Invalid maximum rate sampler epoch duration")
		}
		return bb_otel.NewMaximumRateSampler(
			clock.SystemClock,
			int(policy.MaximumRate.SamplesPerEpoch),
			epochDuration.AsDuration()), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Unknown sampling policy")
	}
}
