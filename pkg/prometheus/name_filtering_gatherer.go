package prometheus

import (
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
)

type nameFilteringGatherer struct {
	base        prometheus.Gatherer
	namePattern *regexp.Regexp
}

// NewNameFilteringGatherer creates a decorator for Gatherer that is
// capable of filtering metrics by name, using a regular expression
// pattern.
func NewNameFilteringGatherer(base prometheus.Gatherer, namePattern *regexp.Regexp) prometheus.Gatherer {
	return &nameFilteringGatherer{
		base:        base,
		namePattern: namePattern,
	}
}

func (g *nameFilteringGatherer) Gather() ([]*io_prometheus_client.MetricFamily, error) {
	allFamilies, err := g.base.Gather()
	if err != nil {
		return nil, err
	}
	filteredFamilies := make([]*io_prometheus_client.MetricFamily, 0, len(allFamilies))
	for _, family := range allFamilies {
		if family.Name != nil && g.namePattern.MatchString(*family.Name) {
			filteredFamilies = append(filteredFamilies, family)
		}
	}
	return filteredFamilies, nil
}
