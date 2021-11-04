package http

import (
	"net/http"
	"net/url"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/http"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// NewRoundTripperFromConfiguration makes a new HTTP RoundTripper on
// parameters provided in a configuration file.
func NewRoundTripperFromConfiguration(configuration *pb.ClientConfiguration) (http.RoundTripper, error) {
	tlsConfig, err := util.NewTLSConfigFromClientConfiguration(configuration.GetTls())
	if err != nil {
		return nil, err
	}
	defaultTransport := http.Transport{
		TLSClientConfig: tlsConfig,
	}
	if proxyURL := configuration.GetProxyUrl(); proxyURL != "" {
		parsedProxyURL, err := url.Parse(proxyURL)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse proxy URL")
		}
		defaultTransport.Proxy = http.ProxyURL(parsedProxyURL)
	}
	var roundTripper http.RoundTripper = &defaultTransport

	if headerValues := configuration.GetAddHeaders(); len(headerValues) > 0 {
		roundTripper = NewHeaderAddingRoundTripper(roundTripper, headerValues)
	}

	return roundTripper, nil
}
