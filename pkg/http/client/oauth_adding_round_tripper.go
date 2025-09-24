package client

import (
	"net/http"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"

	"golang.org/x/oauth2"
)

// NewOAuthAddingRoundTripper is a decorator for RoundTripper that requests a
// OAuth2 token from the TokenSource and add its to the HTTP header on all
// outgoing requests.
func NewOAuthAddingRoundTripper(base http.RoundTripper, config *pb.OAuthConfiguration) (http.RoundTripper, error) {
	source, err := TokenSource(config)
	if err != nil {
		return nil, err
	}
	return &oauth2.Transport{
		Source: source,
		Base:   base,
	}, nil
}
