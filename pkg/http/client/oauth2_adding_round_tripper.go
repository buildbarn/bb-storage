package client

import (
	"net/http"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"

	"golang.org/x/oauth2"
)

// NewOAuth2AddingRoundTripper is a decorator for RoundTripper that requests a
// OAuth2 token from the TokenSource and add its to the HTTP header on all
// outgoing requests.
func NewOAuth2AddingRoundTripper(base http.RoundTripper, config *pb.OAuth2Configuration) (http.RoundTripper, error) {
	source, err := NewOAuth2TokenSourceFromConfiguration(config)
	if err != nil {
		return nil, err
	}
	return &oauth2.Transport{
		Source: source,
		Base:   base,
	}, nil
}
