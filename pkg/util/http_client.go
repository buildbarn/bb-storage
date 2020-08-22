package util

import (
	"net/http"

	otelhttp "go.opentelemetry.io/contrib/instrumentation/net/http"
)

// HTTPClient is an interface around Go's standard HTTP client type. It
// has been added to aid unit testing.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// DefaultHTTPClient includes net/http instrumentation from
// OpenTelemetry, for propagation and span generation.
var DefaultHTTPClient HTTPClient = &http.Client{
	Transport: otelhttp.NewTransport(http.DefaultTransport),
}
