package prometheus_test

import (
	"regexp"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/prometheus"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestNameFilteringGatherer(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseGatherer := mock.NewMockPrometheusGatherer(ctrl)
	gatherer := prometheus.NewNameFilteringGatherer(
		baseGatherer,
		regexp.MustCompile("^node_"))

	t.Run("Success", func(t *testing.T) {
		baseGatherer.EXPECT().Gather().Return([]*io_prometheus_client.MetricFamily{
			{
				Name: ptr("go_goroutines"),
				Help: ptr("Number of goroutines that currently exist."),
				Type: ptr(io_prometheus_client.MetricType_GAUGE),
				Metric: []*io_prometheus_client.Metric{{
					Gauge: &io_prometheus_client.Gauge{
						Value: ptr(8.0),
					},
				}},
			},
			{
				Name: ptr("node_network_transmit_packets_total"),
				Help: ptr("Network device statistic transmit_packets."),
				Type: ptr(io_prometheus_client.MetricType_COUNTER),
				Metric: []*io_prometheus_client.Metric{{
					Label: []*io_prometheus_client.LabelPair{{
						Name:  ptr("device"),
						Value: ptr("en0"),
					}},
					Counter: &io_prometheus_client.Counter{
						Value: ptr(262294.0),
					},
				}},
			},
		}, nil)

		families, err := gatherer.Gather()
		require.NoError(t, err)
		require.Len(t, families, 1)
		testutil.RequireEqualProto(t, &io_prometheus_client.MetricFamily{
			Name: ptr("node_network_transmit_packets_total"),
			Help: ptr("Network device statistic transmit_packets."),
			Type: ptr(io_prometheus_client.MetricType_COUNTER),
			Metric: []*io_prometheus_client.Metric{{
				Label: []*io_prometheus_client.LabelPair{{
					Name:  ptr("device"),
					Value: ptr("en0"),
				}},
				Counter: &io_prometheus_client.Counter{
					Value: ptr(262294.0),
				},
			}},
		}, families[0])
	})
}
