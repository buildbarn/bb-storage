package grpc

import (
	"context"
	"crypto/x509"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type tlsClientCertificateAuthenticator struct {
	clientCAs *x509.CertPool
	clock     clock.Clock
	metadata  *auth.AuthenticationMetadata
}

// NewTLSClientCertificateAuthenticator creates an Authenticator that
// only grants access in case the client connected to the gRPC server
// using a TLS client certificate that can be validated against the
// chain of CAs used by the server.
func NewTLSClientCertificateAuthenticator(clientCAs *x509.CertPool, clock clock.Clock, metadata *auth.AuthenticationMetadata) Authenticator {
	return &tlsClientCertificateAuthenticator{
		clientCAs: clientCAs,
		clock:     clock,
		metadata:  metadata,
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
	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		return nil, status.Error(codes.Unauthenticated, "Client provided no TLS client certificate")
	}

	// Perform certificate verification.
	// TODO: Should this be memoized?
	opts := x509.VerifyOptions{
		Roots:         a.clientCAs,
		CurrentTime:   a.clock.Now(),
		Intermediates: x509.NewCertPool(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := certs[0].Verify(opts); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot validate TLS client certificate")
	}
	return a.metadata, nil
}
