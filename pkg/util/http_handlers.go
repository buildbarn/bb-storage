package util

import (
	"net/http"
	// The pprof package does not provide a function for registering
	// its endpoints against an arbitrary mux. Load it to force
	// registration against the default mux, so we can forward
	// traffic to that mux instead.
	_ "net/http/pprof"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterAdministrativeHTTPEndpoints registers HTTP endpoints
// that are used by all Buildbarn services.
func RegisterAdministrativeHTTPEndpoints(router *mux.Router) {
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/-/healthy", func(http.ResponseWriter, *http.Request) {})
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
}
