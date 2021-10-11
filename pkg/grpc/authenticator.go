package grpc

import (
	"context"
	"crypto/x509"

	"github.com/buildbarn/bb-storage/pkg/clock"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authenticator can be used to grant or deny access to a gRPC server.
// Implementations may grant access based on TLS connection state,
// provided headers, source IP address ranges, etc. etc. etc.
type Authenticator interface {
	Authenticate(ctx context.Context) (context.Context, error)
}

// NewAuthenticatorFromConfiguration creates a tree of Authenticator
// objects based on a configuration file.
func NewAuthenticatorFromConfiguration(policy *configuration.AuthenticationPolicy) (Authenticator, error) {
	if policy == nil {
		return nil, status.Error(codes.InvalidArgument, "Authentication policy not specified")
	}
	switch policyKind := policy.Policy.(type) {
	case *configuration.AuthenticationPolicy_Allow:
		return AllowAuthenticator, nil
	case *configuration.AuthenticationPolicy_Any:
		children := make([]Authenticator, 0, len(policyKind.Any.Policies))
		for _, childConfiguration := range policyKind.Any.Policies {
			child, err := NewAuthenticatorFromConfiguration(childConfiguration)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		return NewAnyAuthenticator(children), nil
	case *configuration.AuthenticationPolicy_Deny:
		return NewDenyAuthenticator(policyKind.Deny), nil
	case *configuration.AuthenticationPolicy_TlsClientCertificate:
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM([]byte(policyKind.TlsClientCertificate.ClientCertificateAuthorities)) {
			return nil, status.Error(codes.InvalidArgument, "Failed to parse client certificate authorities")
		}
		return NewTLSClientCertificateAuthenticator(
			clientCAs,
			clock.SystemClock,
			policyKind.TlsClientCertificate.Spiffe,
		), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain an authentication policy type")
	}
}
