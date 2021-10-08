package http

import (
	"net/http"

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
	return &http.Transport{TLSClientConfig: tlsConfig}, nil
}
