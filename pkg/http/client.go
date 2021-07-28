package http

import (
	"net/http"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/tls"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// Client is an interface around Go's standard HTTP client type.
// It has been added to aid unit testing.
type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

// NewClient makes a new HTTP client configured for TLS based on a Protobuf configuration.
func NewClient(tls *configuration.ClientConfiguration) (Client, error) {
	tlsConfig, err := util.NewTLSConfigFromClientConfiguration(tls)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}, nil
}
