package prometheus_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/prometheus"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func ptr[T any](val T) *T {
	return &val
}

func TestHTTPGatherer(t *testing.T) {
	ctrl := gomock.NewController(t)

	roundTripper := mock.NewMockRoundTripper(ctrl)
	gatherer := prometheus.NewHTTPGatherer(
		&http.Client{Transport: roundTripper},
		"http://localhost:9100/metrics")

	t.Run("NotFound", func(t *testing.T) {
		roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "http://localhost:9100/metrics", r.URL.String())
			return &http.Response{
				Status:     "400 Not Found",
				StatusCode: 404,
				Body:       io.NopCloser(bytes.NewBufferString(`404 page not found`)),
			}, nil
		})

		_, err := gatherer.Gather()
		require.Equal(t, errors.New("server returned status \"400 Not Found\", while 200 was expected"), err)
	})

	t.Run("ParseError", func(t *testing.T) {
		roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "http://localhost:9100/metrics", r.URL.String())
			return &http.Response{
				Status:     "200 OK",
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString("These are not valid metrics")),
			}, nil
		})

		_, err := gatherer.Gather()
		require.Error(t, err)
	})

	t.Run("Success", func(t *testing.T) {
		roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "http://localhost:9100/metrics", r.URL.String())
			return &http.Response{
				Status:     "200 OK",
				StatusCode: 200,
				Body: io.NopCloser(bytes.NewBufferString(`
# HELP node_network_transmit_packets_total Network device statistic transmit_packets.
# TYPE node_network_transmit_packets_total counter
node_network_transmit_packets_total{device="en0"} 262294
node_network_transmit_packets_total{device="lo0"} 10
`)),
			}, nil
		})

		families, err := gatherer.Gather()
		require.NoError(t, err)
		require.Len(t, families, 1)
		testutil.RequireEqualProto(t, &io_prometheus_client.MetricFamily{
			Name: ptr("node_network_transmit_packets_total"),
			Help: ptr("Network device statistic transmit_packets."),
			Type: ptr(io_prometheus_client.MetricType_COUNTER),
			Metric: []*io_prometheus_client.Metric{
				{
					Label: []*io_prometheus_client.LabelPair{{
						Name:  ptr("device"),
						Value: ptr("en0"),
					}},
					Counter: &io_prometheus_client.Counter{
						Value: ptr(262294.0),
					},
				},
				{
					Label: []*io_prometheus_client.LabelPair{{
						Name:  ptr("device"),
						Value: ptr("lo0"),
					}},
					Counter: &io_prometheus_client.Counter{
						Value: ptr(10.0),
					},
				},
			},
		}, families[0])
	})
}
