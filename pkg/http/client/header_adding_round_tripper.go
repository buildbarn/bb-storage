package client

import (
	"net/http"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"
)

type headerAddingRoundTripper struct {
	base         http.RoundTripper
	headerValues []*pb.Configuration_HeaderValues
}

// NewHeaderAddingRoundTripper is a decorator for RoundTripper that adds
// additional HTTP header values to all outgoing requests.
func NewHeaderAddingRoundTripper(base http.RoundTripper, headerValues []*pb.Configuration_HeaderValues) http.RoundTripper {
	return &headerAddingRoundTripper{
		base:         base,
		headerValues: headerValues,
	}
}

func (rt *headerAddingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := *req
	newReq.Header = req.Header.Clone()
	for _, headerValues := range rt.headerValues {
		for _, value := range headerValues.Values {
			newReq.Header.Add(headerValues.Header, value)
		}
	}
	return rt.base.RoundTrip(&newReq)
}
