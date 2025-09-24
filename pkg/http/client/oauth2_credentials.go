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

func TokenSource(oauthConfig *configuration.OAuthConfiguration) (oauth2.TokenSource, error) {
	var source oauth2.TokenSource
	var err error

	switch credentials := oauthConfig.Credentials.(type) {
	case *configuration.OAuthConfiguration_ClientCredentials:
		source, err = ClientCredentialsTokenSource(oauthConfig.Scopes, credentials)
	default:
		return nil, status.Error(codes.InvalidArgument, "oauth http client credentials are wrong: only clientCredentials should be provided")
	}
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create oauth configuration")
	}
	return source, err
}

func ClientCredentialsTokenSource(scopes []string, config *configuration.OAuthConfiguration_ClientCredentials) (oauth2.TokenSource, error) {
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
