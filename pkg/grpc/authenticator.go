package grpc

import (
	"context"
	"crypto/x509"

	"github.com/buildbarn/bb-storage/pkg/clock"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authenticator can be used to grant or deny access to a gRPC server.
// Implementations may grant access based on TLS connection state,
// provided headers, source IP address ranges, etc. etc. etc.
type Authenticator interface {
	Authenticate(ctx context.Context) error
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
			clock.SystemClock), nil
	case *configuration.AuthenticationPolicy_Jwt:
		key, err := loadJWTPublicKey([]byte(policyKind.Jwt.Key))
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse JWT public key from string")
		}
		jwtKey := JWTKeyConfig{
			Key: key,
		}
		return NewJWTAuthenticator(jwtKey, clock.SystemClock), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain an authentication policy type")
	}
}

// NewAuthenticatingUnaryInterceptor creates a gRPC request interceptor
// for unary calls that passes all requests through an Authenticator.
// This may be used to enable authentication support on a gRPC server.
func NewAuthenticatingUnaryInterceptor(a Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if err := a.Authenticate(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// NewAuthenticatingStreamInterceptor creates a gRPC request interceptor
// for streaming calls that passes all requests through an
// Authenticator. This may be used to enable authentication support on a
// gRPC server.
func NewAuthenticatingStreamInterceptor(a Authenticator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := a.Authenticate(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}
