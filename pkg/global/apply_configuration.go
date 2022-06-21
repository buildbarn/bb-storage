package global

import (
	"context"
	"io"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	// The pprof package does not provide a function for registering
	// its endpoints against an arbitrary mux. Load it to force
	// registration against the default mux, so we can forward
	// traffic to that mux instead.
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	bb_otel "github.com/buildbarn/bb-storage/pkg/otel"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/global"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/gorilla/mux"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	stateNotServing int32 = iota
	stateServing
)

// ExitCodeInterrupted is used to signal a successful controlled shutdown.
var ExitCodeInterrupted int = 8

// DiagnosticsServer is returned by ApplyConfiguration. It can be used by
// the caller to report whether the application has started up
// successfully.
type DiagnosticsServer struct {
	config                          *pb.DiagnosticsHTTPServerConfiguration
	activeSpansReportingHTTPHandler *bb_otel.ActiveSpansReportingHTTPHandler
	state                           int32
	server                          *http.Server
}

// Serve can be called to report that the program has started successfully.
// The application should now be reported as being healthy and ready, according
// to isReady, and receive incoming requests if applicable.
func (ds *DiagnosticsServer) Serve(terminationContext context.Context) error {
	// Start a diagnostics web server that exposes Prometheus
	// metrics and provides a health check endpoint.
	if ds.config != nil {
		router := mux.NewRouter()
		router.HandleFunc("/-/healthy", func(http.ResponseWriter, *http.Request) {})
		router.HandleFunc("/-/ready", func(w http.ResponseWriter, _ *http.Request) {
			if ds.state == stateServing {
				w.WriteHeader(http.StatusOK)
			} else {
				http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			}
		})
		if ds.config.EnablePrometheus {
			router.Handle("/metrics", promhttp.Handler())
		}
		if ds.config.EnablePprof {
			router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
		}
		if httpHandler := ds.activeSpansReportingHTTPHandler; httpHandler != nil {
			router.Handle("/active_spans", httpHandler)
		}

		ds.server = &http.Server{
			Addr:    ds.config.ListenAddress,
			Handler: router,
		}
		go func() {
			<-terminationContext.Done()
			ds.SetNotServing()
			ds.server.Shutdown(terminationContext)
		}()
		if err := ds.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
	} else {
		<-terminationContext.Done()
	}
	return nil
}

// SetReady updates the health probe to report healthy and ready.
func (ds *DiagnosticsServer) SetReady() {
	ds.state = stateServing
}

// SetNotServing updates the health probe to report healthy but not ready.
func (ds *DiagnosticsServer) SetNotServing() {
	ds.state = stateNotServing
}

// ServeDiagnostics is a wrapper that calls DiagnosticsServer.Serve inside
// a goroutine, managed by the provided errgroup.Group, and returns
// immediately.
func ServeDiagnostics(terminationContext context.Context, terminationGroup *errgroup.Group, diagnosticsServer *DiagnosticsServer) {
	terminationGroup.Go(func() error {
		if err := diagnosticsServer.Serve(terminationContext); err != nil {
			return util.StatusWrap(err, "Diagnostics server")
		}
		return nil
	})
}

// ApplyConfiguration applies configuration options to the running
// process. These configuration options are global, in that they apply
// to all Buildbarn binaries, regardless of their purpose.
func ApplyConfiguration(configuration *pb.Configuration) (*DiagnosticsServer, bb_grpc.ClientFactory, error) {
	// Set the umask, if requested.
	if setUmaskConfiguration := configuration.GetSetUmask(); setUmaskConfiguration != nil {
		if err := setUmask(setUmaskConfiguration.Umask); err != nil {
			return nil, nil, util.StatusWrap(err, "Failed to set umask")
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
			fields := tracingConfiguration.ResourceAttributes
			resourceAttributes := make([]attribute.KeyValue, 0, len(fields))
			for key, value := range fields {
				switch kind := value.Kind.(type) {
				case *pb.TracingConfiguration_ResourceAttributeValue_Bool:
					resourceAttributes = append(resourceAttributes, attribute.Bool(key, kind.Bool))
				case *pb.TracingConfiguration_ResourceAttributeValue_Int64:
					resourceAttributes = append(resourceAttributes, attribute.Int64(key, kind.Int64))
				case *pb.TracingConfiguration_ResourceAttributeValue_Float64:
					resourceAttributes = append(resourceAttributes, attribute.Float64(key, kind.Float64))
				case *pb.TracingConfiguration_ResourceAttributeValue_String_:
					resourceAttributes = append(resourceAttributes, attribute.String(key, kind.String_))
				case *pb.TracingConfiguration_ResourceAttributeValue_BoolArray_:
					resourceAttributes = append(resourceAttributes, attribute.BoolSlice(key, kind.BoolArray.Values))
				case *pb.TracingConfiguration_ResourceAttributeValue_Int64Array_:
					resourceAttributes = append(resourceAttributes, attribute.Int64Slice(key, kind.Int64Array.Values))
				case *pb.TracingConfiguration_ResourceAttributeValue_Float64Array_:
					resourceAttributes = append(resourceAttributes, attribute.Float64Slice(key, kind.Float64Array.Values))
				case *pb.TracingConfiguration_ResourceAttributeValue_StringArray_:
					resourceAttributes = append(resourceAttributes, attribute.StringSlice(key, kind.StringArray.Values))
				default:
					return nil, nil, status.Error(codes.InvalidArgument, "Resource attribute is of an unknown type")
				}
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
		pusher := push.New(pushgateway.Url, pushgateway.Job)
		pusher.Gatherer(prometheus.DefaultGatherer)
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

		go func() {
			for {
				if err := pusher.Push(); err != nil {
					log.Print("Failed to push metrics to Prometheus Pushgateway: ", err)
				}
				time.Sleep(pushIntervalDuration)
			}
		}()
	}

	return &DiagnosticsServer{
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

// InstallTerminationSignalHandler starts watching for SIGTERM and SIGINT. The
// first signal received will cancel the returned context. If a second signal
// is received, the program will exit immediately.
func InstallTerminationSignalHandler() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	// Catch SIGINT and SIGTERM to gracefully shutdown.
	c := make(chan os.Signal)
	signalsToCapture := []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Notify(c, signalsToCapture...)
	go func() {
		sig := <-c
		log.Printf("Caught signal %q, shutting down", sig)
		cancel()
		// A second signal means immediate termination.
		signal.Reset(signalsToCapture...)
	}()
	return ctx
}
