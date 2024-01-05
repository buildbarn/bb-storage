package grpcauth

import (
	"context"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/spiffe"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type mtlsPeerCertificateAuthenticator struct {
	mu              sync.Mutex
	clientCAs       *x509.CertPool
	clock           clock.Clock
	allowedSubjects *spiffe.SubjectMatcher
	caPathName      string
	caMtime         time.Time
}

// Caller must hold mutex.
func (a *mtlsPeerCertificateAuthenticator) handleRotatedCA() {
	// Check if we need to reload CA certs.
	if a.caPathName != "" {
		fi, err := os.Stat(a.caPathName)
		if err == nil {
			mtime := fi.ModTime()
			if  mtime != a.caMtime {
				// CA certs file has changed, so reload it.
				b, err := ioutil.ReadFile(a.caPathName)
				if err != nil {
					log.Printf("Authenticate: can't read caCerts: %v\n", err)
				} else {
					caCerts := x509.NewCertPool()
					if !caCerts.AppendCertsFromPEM(b) {
						log.Println("Authenticate: invalid server certificate authorities")
					} else {
						a.clientCAs = caCerts
						a.caMtime = mtime
					}
				}
			}
		}
	}
}

// Caller must hold mutex.
func (a *mtlsPeerCertificateAuthenticator) verifyPeer(certs []*x509.Certificate, usage x509.ExtKeyUsage) error {
	// Perform certificate verification.
	if len(certs) == 0 {
		return status.Error(codes.Unauthenticated, "Peer provided no TLS certificate")
	}

	// TODO: Should this be memoized?
	opts := x509.VerifyOptions{
		Roots:         a.clientCAs,
		CurrentTime:   a.clock.Now(),
		Intermediates: x509.NewCertPool(),
		KeyUsages:     []x509.ExtKeyUsage{usage},
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	log.Printf("verifyPeer: currentTime %v cert not before %v not after %v\n", opts.CurrentTime, certs[0].NotBefore, certs[0].NotAfter)
	if _, err := certs[0].Verify(opts); err != nil {
		log.Printf("verifyPeer: Verify failed: %v\n", err)
		return util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot validate TLS certificate")
	}
	log.Printf("verifyPeer: Verify succeeded\n")
	if a.allowedSubjects != nil {
		id, err := x509svid.IDFromCert(certs[0])
		if err != nil {
			return util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot validate TLS certificate as valid x509svid")
		}
		pattern, ok := a.allowedSubjects.AllowedSpiffeIds[id.TrustDomain().String()]
		if !ok {
			return status.Error(codes.Unauthenticated, fmt.Sprintf("Trustdomain %q is not permitted", id.String()))
		}
		match, err := regexp.MatchString(pattern, id.Path())
		if !match {
			log.Printf("verifyPeer: SPIFFE Mismatch trust domain %s, path %s, matcher %s\n", id.TrustDomain().String(), id.Path(), pattern)
			return status.Error(codes.Unauthenticated, fmt.Sprintf("Subject %q is not permitted", id.String()))
		}
		if err != nil {
			return util.StatusWrapWithCode(err, codes.Unauthenticated, "Invalid subject match pattern")
		}
	}
	return nil
}

func (a *mtlsPeerCertificateAuthenticator) Authenticate(ctx context.Context) (context.Context, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.handleRotatedCA()

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
		return nil, status.Error(codes.Unauthenticated, "Peer provided no TLS certificate")
	}
	// log.Printf("Authenticate: extracting certs from context; certs[0] = %#v\n", certs[0])
	err := a.verifyPeer(certs, x509.ExtKeyUsageClientAuth)
	if err != nil {
		log.Printf("Authenticate: verifyPeer failed: %v\n", err)
		return nil, err
	}
	log.Printf("Authenticate: success!\n")
	return ctx, nil
}

type MTLSVerifier interface {
	GetVerifyCertificate(usage x509.ExtKeyUsage) func(rawCerts [][]byte, _ [][]*x509.Certificate) (error)
}

func NewMTLSCertificateVerifier(clientCAs *x509.CertPool, clock clock.Clock, allowedSubjects *spiffe.SubjectMatcher, caPathName string) MTLSVerifier {
	var mtime time.Time
	if caPathName != "" {
		fi, err := os.Stat(caPathName)
		if err == nil {
			mtime = fi.ModTime()
		} else {
			log.Printf("NewMTLSCertificateVerifier: can't stat %s: %v\n", caPathName, err)
		}
	}
	return &mtlsPeerCertificateAuthenticator{
		allowedSubjects: allowedSubjects,
		clientCAs:       clientCAs,
		clock:           clock,
		caPathName:      caPathName,
		caMtime:         mtime,
	}
}

// Used for additional logic during TLS handshake to implement mTLS.  See tls.Config.VerifyPeerCertificate.
func (a *mtlsPeerCertificateAuthenticator) GetVerifyCertificate(usage x509.ExtKeyUsage) func(rawCerts [][]byte, _ [][]*x509.Certificate) (error) {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) (error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.handleRotatedCA()

		var certs []*x509.Certificate
		for _, rawCert := range rawCerts {
			cert, err := x509.ParseCertificate(rawCert)
			if err != nil {
				return util.StatusWrapWithCode(err, codes.Unauthenticated, "Cannot parse TLS certificate")
			}
			certs = append(certs, cert)
		}
		if len(certs) != 0 {
			log.Printf("MTLS Serial Number = %s Not Before %v Not After %v\n", certs[0].SerialNumber.String(), certs[0].NotBefore, certs[0].NotAfter)
		}
		err := a.verifyPeer(certs, usage)
		if usage == x509.ExtKeyUsageClientAuth {
			log.Printf("MTLS: client auth ret %v\n", err)
		} else if usage == x509.ExtKeyUsageServerAuth {
			log.Printf("MTLS: server auth ret %v\n", err)
		} else {
			log.Printf("MTLS: UNKNOWN auth ret %v\n", err)
		}
		return err
	}
}
