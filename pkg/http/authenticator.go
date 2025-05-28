package http

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/http"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Authenticator can be used to grant or deny access to a HTTP server.
// Implementations may grant access based on TLS connection state,
// provided headers, source IP address ranges, etc. etc. etc.
type Authenticator interface {
	Authenticate(w http.ResponseWriter, r *http.Request) (*auth.AuthenticationMetadata, error)
}

// NewAuthenticatorFromConfiguration creates a tree of Authenticator
// objects based on a configuration file.
func NewAuthenticatorFromConfiguration(policy *configuration.AuthenticationPolicy, group program.Group, grpcClientFactory grpc.ClientFactory) (Authenticator, error) {
	if policy == nil {
		return nil, status.Error(codes.InvalidArgument, "Authentication policy not specified")
	}
	switch policyKind := policy.Policy.(type) {
	case *configuration.AuthenticationPolicy_Allow:
		authenticationMetadata, err := auth.NewAuthenticationMetadataFromProto(policyKind.Allow)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "Failed to create authentication metadata")
		}
		return NewAllowAuthenticator(authenticationMetadata), nil
	case *configuration.AuthenticationPolicy_Any:
		children := make([]Authenticator, 0, len(policyKind.Any.Policies))
		for _, childConfiguration := range policyKind.Any.Policies {
			child, err := NewAuthenticatorFromConfiguration(childConfiguration, group, grpcClientFactory)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		return NewAnyAuthenticator(children), nil
	case *configuration.AuthenticationPolicy_Deny:
		return NewDenyAuthenticator(policyKind.Deny), nil
	case *configuration.AuthenticationPolicy_Jwt:
		authorizationHeaderParser, err := jwt.NewAuthorizationHeaderParserFromConfiguration(policyKind.Jwt, group)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create authorization header parser for JWT authentication policy")
		}
		authenticator, err := NewRequestHeadersAuthenticator(authorizationHeaderParser, []string{jwt.AuthorizationHeaderName})
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create request headers authenticator for JWT authentication policy")
		}
		return authenticator, nil
	case *configuration.AuthenticationPolicy_Oidc:
		// Select a name and encryption key for the session
		// state cookie. Even though the configuration has a
		// dedicated cookie seed field, we include the rest of
		// the configuration message as well. This ensures that
		// any changes to the configuration automatically
		// invalidate existing sessions.
		if len(policyKind.Oidc.CookieSeed) == 0 {
			return nil, status.Error(codes.InvalidArgument, "No OIDC cookie seed provided")
		}
		fullCookieSeed, err := proto.MarshalOptions{Deterministic: true}.Marshal(policyKind.Oidc)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "Failed to marshal configuration to compute OIDC cookie seed")
		}
		cookieSeedHash := sha256.Sum256(fullCookieSeed)

		// Let the first 128 bits of the seed hash be the name
		// of the cookie, while the last 128 bits are used as
		// the AES key for encrypting/signing the cookie value.
		cookieName := base64.RawURLEncoding.EncodeToString(cookieSeedHash[:sha256.Size/2])
		cookieCipher, err := aes.NewCipher(cookieSeedHash[sha256.Size/2:])
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create OIDC cookie encryption block cipher")
		}
		cookieAEAD, err := cipher.NewGCM(cookieCipher)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create OIDC cookie encryption block cipher mode of operation")
		}

		metadataExtractor, err := jmespath.Compile(policyKind.Oidc.MetadataExtractionJmespathExpression)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to compile OIDC metadata extraction JMESPath expression")
		}
		roundTripper, err := NewRoundTripperFromConfiguration(policyKind.Oidc.HttpClient)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create OIDC HTTP client")
		}

		oauth2Config := &oauth2.Config{
			ClientID:     policyKind.Oidc.ClientId,
			ClientSecret: policyKind.Oidc.ClientSecret,
			Endpoint: oauth2.Endpoint{
				AuthURL:  policyKind.Oidc.AuthorizationEndpointUrl,
				TokenURL: policyKind.Oidc.TokenEndpointUrl,
			},
			RedirectURL: policyKind.Oidc.RedirectUrl,
			Scopes:      policyKind.Oidc.Scopes,
		}

		var oidcClaimsFetcher OIDCClaimsFetcher
		switch userInfoSource := policyKind.Oidc.UserInfoSource.(type) {
		case *configuration.OIDCAuthenticationPolicy_UserInfoEndpointUrl:
			oidcClaimsFetcher = NewUserInfoOIDCClaimsFetcher(
				oauth2Config,
				userInfoSource.UserInfoEndpointUrl,
			)
		case *configuration.OIDCAuthenticationPolicy_UseIdTokenClaims:
			oidcClaimsFetcher = IDTokenOIDCClaimsFetcher
		default:
			return nil, status.Error(codes.InvalidArgument, "OIDC user info source not specified")
		}

		return NewOIDCAuthenticator(
			oauth2Config,
			oidcClaimsFetcher,
			metadataExtractor,
			&http.Client{
				Transport: NewMetricsRoundTripper(roundTripper, "OIDCAuthenticator"),
			},
			random.CryptoThreadSafeGenerator,
			cookieName,
			cookieAEAD,
			clock.SystemClock)
	case *configuration.AuthenticationPolicy_AcceptHeader:
		base, err := NewAuthenticatorFromConfiguration(policyKind.AcceptHeader.Policy, group, grpcClientFactory)
		if err != nil {
			return nil, err
		}
		return NewAcceptHeaderAuthenticator(base, policyKind.AcceptHeader.MediaTypes), nil
	case *configuration.AuthenticationPolicy_Remote:
		// TODO: With auth.RequestHeadersPolicy = oneof {auth.Jwt, auth.Remote}
		// in the .proto definitions, the HTTP and gRPC authentication policy
		// code could be unified. Unfortunately, that creates the .proto
		// dependency cycle below:
		//
		//     grpc.ServerConfiguration ->
		//     grpc.AuthenticationPolicy ->
		//     auth.RequestHeadersAuthenticator ->
		//     auth.RemoteAuthenticator ->
		//     grpc.ClientConfiguration
		//
		// Resolving this requires splitting `grpc.proto` into `grpc_client.proto`,
		// `grpc_server.proto` and `grpc_tracing_method.proto`.
		backend, err := grpc.NewRemoteRequestHeadersAuthenticatorFromConfiguration(policyKind.Remote, grpcClientFactory)
		if err != nil {
			return nil, err
		}
		return NewRequestHeadersAuthenticator(backend, policyKind.Remote.Headers)
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain an authentication policy type")
	}
}
