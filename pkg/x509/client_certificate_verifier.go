package x509

import (
	"crypto/x509"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClientCertificateVerifier can be used to verify X.509 client
// certificates. Furthermore, it can construct authentication metadata
// that contains attributes specified in the client certificate (e.g.,
// DNS names or email addresses).
type ClientCertificateVerifier struct {
	clientCAs         *x509.CertPool
	clock             clock.Clock
	validator         *jmespath.JMESPath
	metadataExtractor *jmespath.JMESPath
}

// NewClientCertificateVerifier creates a ClientCertificateVerifier that
// verifies X.509 client certificates using the provided certificate
// authorities. Authentication metadata is constructed using the
// provided JMESPath expression.
func NewClientCertificateVerifier(clientCAs *x509.CertPool, clock clock.Clock, validator, metadataExtractor *jmespath.JMESPath) *ClientCertificateVerifier {
	return &ClientCertificateVerifier{
		clientCAs:         clientCAs,
		clock:             clock,
		validator:         validator,
		metadataExtractor: metadataExtractor,
	}
}

// VerifyClientCertificate verifies that a chain of certificates
// provided by a client are valid. Upon success, authentication metadata
// that's based on attributes of the certificate is returned.
func (v *ClientCertificateVerifier) VerifyClientCertificate(certs []*x509.Certificate) (*auth.AuthenticationMetadata, error) {
	if len(certs) == 0 {
		return nil, status.Error(codes.Unauthenticated, "Client provided no X.509 client certificate")
	}

	// Perform certificate verification.
	// TODO: Should this be memoized?
	opts := x509.VerifyOptions{
		Roots:         v.clientCAs,
		CurrentTime:   v.clock.Now(),
		Intermediates: x509.NewCertPool(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := certs[0].Verify(opts); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot validate X.509 client certificate")
	}

	searchContext := getClientCertificateJMESPathSearchContext(certs[0])

	// Validate the client cert matches our expectations.
	validationResult, err := v.validator.Search(searchContext)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot validate X.509 client certificate claims")
	}
	if validationResult != true {
		return nil, status.Error(codes.Unauthenticated, "Rejected X.509 client certificate claims")
	}

	// Extract metadata from the client cert.
	metadataRaw, err := v.metadataExtractor.Search(searchContext)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot extract metadata from X.509 client certificate")
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
