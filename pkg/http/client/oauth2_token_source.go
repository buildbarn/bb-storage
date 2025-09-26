package client

import (
	"context"
	"net/http"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewOAuth2TokenSourceFromConfiguration uses the given configuration to create a
// token source for HTTP clients.
func NewOAuth2TokenSourceFromConfiguration(oauthConfig *configuration.OAuth2Configuration) (oauth2.TokenSource, error) {
	var source oauth2.TokenSource
	var err error

	switch credentials := oauthConfig.Credentials.(type) {
	case *configuration.OAuth2Configuration_ClientCredentials:
		source, err = clientCredentialsTokenSource(oauthConfig.Scopes, credentials)
	default:
		return nil, status.Error(codes.InvalidArgument, "oauth credentials are wrong")
	}
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create oauth configuration")
	}
	return source, err
}

func clientCredentialsTokenSource(scopes []string, config *configuration.OAuth2Configuration_ClientCredentials) (oauth2.TokenSource, error) {
	roundTripper, err := NewRoundTripperFromConfiguration(config.ClientCredentials.HttpClient)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create HTTP client")
	}
	httpClient := &http.Client{
		Transport: NewMetricsRoundTripper(roundTripper, "ClientCredentials"),
	}
	oidcConfig := &clientcredentials.Config{
		ClientID:     config.ClientCredentials.ClientId,
		ClientSecret: config.ClientCredentials.ClientSecret,
		TokenURL:     config.ClientCredentials.TokenEndpointUrl,
		Scopes:       scopes,
	}
	return oidcConfig.TokenSource(
		context.WithValue(context.Background(), oauth2.HTTPClient, httpClient),
	), nil
}
