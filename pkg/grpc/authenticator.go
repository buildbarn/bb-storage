package grpc

import (
	"context"
	"crypto/x509"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/jwt"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authenticator can be used to grant or deny access to a gRPC server.
// Implementations may grant access based on TLS connection state,
// provided headers, source IP address ranges, etc. etc. etc.
type Authenticator interface {
	Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error)
}

// NewAuthenticatorFromConfiguration creates a tree of Authenticator
// objects based on a configuration file.
func NewAuthenticatorFromConfiguration(policy *configuration.AuthenticationPolicy) (Authenticator, bool, error) {
	if policy == nil {
		return nil, false, status.Error(codes.InvalidArgument, "Authentication policy not specified")
	}
	switch policyKind := policy.Policy.(type) {
	case *configuration.AuthenticationPolicy_Allow:
		authenticationMetadata, err := auth.NewAuthenticationMetadataFromProto(policyKind.Allow)
		if err != nil {
			return nil, false, status.Error(codes.InvalidArgument, "Failed to create authentication metadata")
		}
		return NewAllowAuthenticator(authenticationMetadata), false, nil
	case *configuration.AuthenticationPolicy_Any:
		children := make([]Authenticator, 0, len(policyKind.Any.Policies))
		needsPeerTransportCredentials := false
		for _, childConfiguration := range policyKind.Any.Policies {
			child, childNeedsPeerTransportCredentials, err := NewAuthenticatorFromConfiguration(childConfiguration)
			if err != nil {
				return nil, false, err
			}
			children = append(children, child)
			needsPeerTransportCredentials = needsPeerTransportCredentials || childNeedsPeerTransportCredentials
		}
		return NewAnyAuthenticator(children), needsPeerTransportCredentials, nil
	case *configuration.AuthenticationPolicy_Deny:
		return NewDenyAuthenticator(policyKind.Deny), false, nil
	case *configuration.AuthenticationPolicy_TlsClientCertificate:
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM([]byte(policyKind.TlsClientCertificate.ClientCertificateAuthorities)) {
			return nil, false, status.Error(codes.InvalidArgument, "Failed to parse client certificate authorities")
		}
		validator, err := jmespath.Compile(policyKind.TlsClientCertificate.ValidationJmespathExpression)
		if err != nil {
			return nil, false, util.StatusWrap(err, "Failed to compile validation JMESPath expression")
		}
		metadataExtractor, err := jmespath.Compile(policyKind.TlsClientCertificate.MetadataExtractionJmespathExpression)
		if err != nil {
			return nil, false, util.StatusWrap(err, "Failed to compile metadata extraction JMESPath expression")
		}
		return NewTLSClientCertificateAuthenticator(
			clientCAs,
			clock.SystemClock,
			validator,
			metadataExtractor,
		), false, nil
	case *configuration.AuthenticationPolicy_Jwt:
		authorizationHeaderParser, err := jwt.NewAuthorizationHeaderParserFromConfiguration(policyKind.Jwt)
		if err != nil {
			return nil, false, util.StatusWrap(err, "Failed to create authorization header parser for JWT authentication policy")
		}
		return NewJWTAuthenticator(authorizationHeaderParser), false, nil
	case *configuration.AuthenticationPolicy_PeerCredentialsJmespathExpression:
		metadataExtractor, err := jmespath.Compile(policyKind.PeerCredentialsJmespathExpression)
		if err != nil {
			return nil, false, util.StatusWrap(err, "Failed to compile peer credentials metadata extraction JMESPath expression")
		}
		return NewPeerCredentialsAuthenticator(metadataExtractor), true, nil
	default:
		return nil, false, status.Error(codes.InvalidArgument, "Configuration did not contain an authentication policy type")
	}
}
