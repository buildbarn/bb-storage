package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/x509"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type tlsClientCertificateAuthenticator struct {
	verifier *x509.ClientCertificateVerifier
}

// NewTLSClientCertificateAuthenticator creates an Authenticator that
// only grants access in case the client connected to the gRPC server
// using a TLS client certificate that can be validated against the
// chain of CAs used by the server.
func NewTLSClientCertificateAuthenticator(verifier *x509.ClientCertificateVerifier) Authenticator {
	return &tlsClientCertificateAuthenticator{
		verifier: verifier,
	}
}

func (a *tlsClientCertificateAuthenticator) Authenticate(ctx context.Context) (*auth.AuthenticationMetadata, error) {
	// Extract client certificate chain from the connection.
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "Connection was not established using gRPC")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "Connection was not established using TLS")
	}
	return a.verifier.VerifyClientCertificate(tlsInfo.State.PeerCertificates)
}
