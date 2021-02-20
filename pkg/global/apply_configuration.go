package global

import (
	"io"
	"log"
	"net/http"
	// The pprof package does not provide a function for registering
	// its endpoints against an arbitrary mux. Load it to force
	// registration against the default mux, so we can forward
	// traffic to that mux instead.
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/global"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"contrib.go.opencensus.io/exporter/jaeger"
	prometheus_exporter "contrib.go.opencensus.io/exporter/prometheus"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

// LifecycleState is returned by ApplyConfiguration. It can be used by
// the caller to report whether the application has started up
// successfully.
type LifecycleState struct {
	config *pb.DiagnosticsHTTPServerConfiguration
}

// MarkReadyAndWait can be called to report that the program has started
// successfully. The application should now be reported as being healthy
// and ready, and receive incoming requests if applicable.
func (ls *LifecycleState) MarkReadyAndWait() {
	// Start a diagnostics web server that exposes Prometheus
	// metrics and provides a health check endpoint.
	if ls.config == nil {
		select {}
	} else {
		router := mux.NewRouter()
		router.HandleFunc("/-/healthy", func(http.ResponseWriter, *http.Request) {})
		if ls.config.EnablePrometheus {
			router.Handle("/metrics", promhttp.Handler())
		}
		if ls.config.EnablePprof {
			router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
		}

		log.Fatal(http.ListenAndServe(ls.config.ListenAddress, router))
	}
}

// ApplyConfiguration applies configuration options to the running
// process. These configuration options are global, in that they apply
// to all Buildbarn binaries, regardless of their purpose.
func ApplyConfiguration(configuration *pb.Configuration) (*LifecycleState, error) {
	// Set the umask, if requested.
	if setUmaskConfiguration := configuration.GetSetUmask(); setUmaskConfiguration != nil {
		if err := setUmask(setUmaskConfiguration.Umask); err != nil {
			return nil, util.StatusWrap(err, "Failed to set umask")
		}
	}

	// Logging.
	logPaths := configuration.GetLogPaths()
	logWriters := append(make([]io.Writer, 0, len(logPaths)+1), os.Stderr)
	for _, logPath := range logPaths {
		w, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to open log path %#v", logPath)
		}
		logWriters = append(logWriters, w)
	}
	log.SetOutput(io.MultiWriter(logWriters...))

	// Push traces to Jaeger.
	if tracingConfiguration := configuration.GetTracing(); tracingConfiguration != nil {
		if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
			return nil, util.StatusWrap(err, "Failed to register ocgrpc server views")
		}

		if jaegerConfiguration := tracingConfiguration.Jaeger; jaegerConfiguration != nil {
			je, err := jaeger.NewExporter(jaeger.Options{
				AgentEndpoint:     jaegerConfiguration.AgentEndpoint,
				CollectorEndpoint: jaegerConfiguration.CollectorEndpoint,
				Process: jaeger.Process{
					ServiceName: jaegerConfiguration.ServiceName,
				},
			})
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to create the Jaeger exporter")
			}
			trace.RegisterExporter(je)
		}

		if stackdriverConfiguration := tracingConfiguration.Stackdriver; stackdriverConfiguration != nil {
			defaultTraceAttributes := map[string]interface{}{}
			for k, v := range stackdriverConfiguration.DefaultTraceAttributes {
				defaultTraceAttributes[k] = v
			}
			se, err := stackdriver.NewExporter(stackdriver.Options{
				ProjectID:              stackdriverConfiguration.ProjectId,
				Location:               stackdriverConfiguration.Location,
				DefaultTraceAttributes: defaultTraceAttributes,
			})
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to create the Stackdriver exporter")
			}
			trace.RegisterExporter(se)
		}

		if tracingConfiguration.EnablePrometheus {
			pe, err := prometheus_exporter.NewExporter(prometheus_exporter.Options{
				Registry:  prometheus.DefaultRegisterer.(*prometheus.Registry),
				Namespace: "bb_storage",
			})
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to create the Prometheus stats exporter")
			}
			view.RegisterExporter(pe)
		}

		if samplingPolicy := tracingConfiguration.SamplingPolicy; samplingPolicy != nil {
			switch policy := samplingPolicy.(type) {
			case *pb.TracingConfiguration_SampleAlways:
				trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

			case *pb.TracingConfiguration_SampleNever:
				trace.ApplyConfig(trace.Config{DefaultSampler: trace.NeverSample()})

			case *pb.TracingConfiguration_SampleProbability:
				trace.ApplyConfig(trace.Config{DefaultSampler: trace.ProbabilitySampler(policy.SampleProbability)})

			default:
				return nil, status.Error(codes.InvalidArgument, "Failed to decode sampling policy from configuration")
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
		config: configuration.GetDiagnosticsHttpServer(),
	}, nil
}
