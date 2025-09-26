package server

import (
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/auth"

	"google.golang.org/grpc/status"
)

type authenticatingHandler struct {
	handler       http.Handler
	authenticator Authenticator
}

// NewAuthenticatingHandler wraps a http.Handler in such a way that all
// requests are processed by an Authenticator. Upon success, the request
// is forwarded to the http.Handler. Upon failure, an error message is
// returned to the client.
func NewAuthenticatingHandler(handler http.Handler, authenticator Authenticator) http.Handler {
	return &authenticatingHandler{
		handler:       handler,
		authenticator: authenticator,
	}
}

func (h *authenticatingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if metadata, err := h.authenticator.Authenticate(w, r); err != nil {
		http.Error(w, err.Error(), StatusCodeFromGRPCCode(status.Code(err)))
	} else if metadata != nil {
		// Authentication succeeded, or the Authenticator did
		// not write a response to the client (e.g., emit a
		// redirect). Forward the request to the underlying
		// handler.
		h.handler.ServeHTTP(w, r.WithContext(auth.NewContextWithAuthenticationMetadata(r.Context(), metadata)))
	}
}
