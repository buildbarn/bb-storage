package global

import (
	"log"
	"runtime"
	"time"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/global"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	"contrib.go.opencensus.io/exporter/jaeger"
	prometheus_exporter "contrib.go.opencensus.io/exporter/prometheus"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.opencensus.io/zpages"
)

// ApplyConfiguration applies configuration options to the running
// process. These configuration options are global, in that they apply
// to all Buildbarn binaries, regardless of their purpose.
func ApplyConfiguration(configuration *pb.Configuration) error {
	// Push traces to Jaeger.
	if tracingConfiguration := configuration.GetTracing(); tracingConfiguration != nil {
		if jaegerConfiguration := tracingConfiguration.GetJaeger(); jaegerConfiguration != nil {
			pe, err := prometheus_exporter.NewExporter(prometheus_exporter.Options{
				Registry:  prometheus.DefaultRegisterer.(*prometheus.Registry),
				Namespace: "bb_storage",
			})
			if err != nil {
				return util.StatusWrap(err, "Failed to create the Prometheus stats exporter")
			}
			view.RegisterExporter(pe)
			if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
				return util.StatusWrap(err, "Failed to register ocgrpc server views")
			}
			zpages.Handle(nil, "/debug")
			je, err := jaeger.NewExporter(jaeger.Options{
				AgentEndpoint:     jaegerConfiguration.AgentEndpoint,
				CollectorEndpoint: jaegerConfiguration.CollectorEndpoint,
				Process: jaeger.Process{
					ServiceName: jaegerConfiguration.ServiceName,
				},
			})
			if err != nil {
				return util.StatusWrap(err, "Failed to create the Jaeger exporter")
			}
			trace.RegisterExporter(je)
		}

		if stackdriverConfiguration := tracingConfiguration.GetStackdriver(); stackdriverConfiguration != nil {
			se, err := stackdriver.NewExporter(stackdriver.Options{
				ProjectID: stackdriverConfiguration.ProjectId,
				Location:  stackdriverConfiguration.Location,
			})
			if err != nil {
				return util.StatusWrap(err, "Failed to create the Stackdriver exporter")
			}
			trace.RegisterExporter(se)
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
			return util.StatusWrap(err, "Failed to parse push interval")
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

	return nil
}
