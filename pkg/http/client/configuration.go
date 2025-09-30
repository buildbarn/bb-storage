package client

import (
	"net"
	"net/http"
	"net/url"
	"time"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// NewRoundTripperFromConfiguration makes a new HTTP RoundTripper on
// parameters provided in a configuration file.
func NewRoundTripperFromConfiguration(configuration *pb.Configuration) (http.RoundTripper, error) {
	tlsConfig, err := util.NewTLSConfigFromClientConfiguration(configuration.GetTls())
	if err != nil {
		return nil, err
	}
	defaultTransport := http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: !configuration.GetDisableHttp2(),
		TLSClientConfig:   tlsConfig,
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

	if oauth2Config := configuration.GetOauth2(); oauth2Config != nil {
		if roundTripper, err = NewOAuth2AddingRoundTripper(roundTripper, oauth2Config); err != nil {
			return nil, util.StatusWrap(err, "Failed to create oauth2 round tripper")
		}
	}
	return roundTripper, nil
}
