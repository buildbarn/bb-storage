package prometheus

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

type httpGatherer struct {
	client *http.Client
	url    string
}

// NewHTTPGatherer creates a Gatherer that is capable of scraping
// metrics from a remote target over HTTP, using the text-based
// exposition format.
func NewHTTPGatherer(client *http.Client, url string) prometheus.Gatherer {
	return &httpGatherer{
		client: client,
		url:    url,
	}
}

func (g *httpGatherer) Gather() ([]*io_prometheus_client.MetricFamily, error) {
	req, err := http.NewRequest(http.MethodGet, g.url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform HTTP request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("server returned status %#v, while 200 was expected", resp.Status)
	}

	// parser.TextToMetricFamilies() returns a dictionary that is
	// keyed by metric name. Gatherer needs to return a slice.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics: %w", err)
	}
	results := make([]*io_prometheus_client.MetricFamily, 0, len(families))
	for _, family := range families {
		results = append(results, family)
	}
	return results, nil
}
