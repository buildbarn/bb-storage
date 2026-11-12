package client

import (
	"net/http"
	"strings"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"
)

// Keep in sync with shouldCopyHeaderOnRedirect() in net/http.
var sensitiveHeaders = map[string]bool{
	"Authorization":    true,
	"Cookie":           true,
	"Cookie2":          true,
	"Www-Authenticate": true,
}

// leftOriginDomain walks req back to the request the caller made. net/http sets
// Response only on a request it built for a redirect, and http.Transport sets
// Response.Request, so the chain terminates at the original. Every hop is
// checked, not just the last, because net/http's decision is sticky.
func leftOriginDomain(req *http.Request) bool {
	origin := req
	for origin.Response != nil {
		if origin.Response.Request == nil {
			// Origin unknown, so withhold rather than guess.
			return true
		}
		origin = origin.Response.Request
	}
	originHost := strings.ToLower(origin.URL.Hostname())
	for hop := req; hop != origin; hop = hop.Response.Request {
		host := strings.ToLower(hop.URL.Hostname())
		if host != originHost && !strings.HasSuffix(host, "."+originHost) {
			return true
		}
	}
	return false
}

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
	// Runs below http.Client, so headers added here escape http.Client's own
	// rule on what may follow a redirect. Apply that rule instead.
	offDomain := leftOriginDomain(req)
	for _, headerValues := range rt.headerValues {
		if offDomain && sensitiveHeaders[http.CanonicalHeaderKey(headerValues.Header)] {
			continue
		}
		for _, value := range headerValues.Values {
			newReq.Header.Add(headerValues.Header, value)
		}
	}
	return rt.base.RoundTrip(&newReq)
}
