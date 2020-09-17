package global

import (
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
func ApplyConfiguration(configuration *pb.Configuration) (*LifecycleState, error) {
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

		if tracingConfiguration.AlwaysSample {
			trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
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
