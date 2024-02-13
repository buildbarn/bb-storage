package grpc

import (
	"context"
	"crypto/x509"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type tlsClientCertificateAuthenticator struct {
	clientCAs         *x509.CertPool
	clock             clock.Clock
	validator         *jmespath.JMESPath
	metadataExtractor *jmespath.JMESPath
}

// NewTLSClientCertificateAuthenticator creates an Authenticator that
// only grants access in case the client connected to the gRPC server
// using a TLS client certificate that can be validated against the
// chain of CAs used by the server.
func NewTLSClientCertificateAuthenticator(clientCAs *x509.CertPool, clock clock.Clock, validator, metadataExtractor *jmespath.JMESPath) Authenticator {
	return &tlsClientCertificateAuthenticator{
		clientCAs:         clientCAs,
		clock:             clock,
		validator:         validator,
		metadataExtractor: metadataExtractor,
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

	searchContext := getClientCertificateJMESPathSearchContext(certs[0])

	// Validate the client cert matches our expectations.
	validationResult, err := a.validator.Search(searchContext)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot validate TLS client certificate claims")
	}
	if validationResult != true {
		return nil, status.Error(codes.Unauthenticated, "Rejected TLS client certificate claims")
	}

	// Extract metadata from the client cert.
	metadataRaw, err := a.metadataExtractor.Search(searchContext)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot extract metadata from TLS client certificate")
	}

	return auth.NewAuthenticationMetadataFromRaw(metadataRaw)
}

func getClientCertificateJMESPathSearchContext(cert *x509.Certificate) map[string]any {
	// We have to go through this copying and json dance in order to
	// ensure that we don't replace [] with null, and that we have the proper
	// types needed for JMESPath to search over without typing failures.

	dnsNames := make([]any, 0, len(cert.DNSNames))
	for _, d := range cert.DNSNames {
		dnsNames = append(dnsNames, d)
	}
	emailAddresses := make([]any, 0, len(cert.EmailAddresses))
	for _, e := range cert.EmailAddresses {
		emailAddresses = append(emailAddresses, e)
	}

	uris := make([]any, 0, len(cert.URIs))
	for _, e := range cert.URIs {
		uris = append(uris, e.String())
	}

	// The data structure that users can search over
	searchContext := map[string]any{
		"dnsNames":       dnsNames,
		"emailAddresses": emailAddresses,
		"uris":           uris,
	}

	return searchContext
}
