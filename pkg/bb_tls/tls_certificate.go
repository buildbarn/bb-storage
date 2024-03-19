package bb_tls

import (
	"bytes"
	"crypto/tls"
	"os"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// RotatingTLSCertificate provides an up-to-date certificate given file paths and a refresh interval.
type RotatingTLSCertificate struct {
	certFile string
	keyFile  string

	lock     sync.RWMutex
	cert     *tls.Certificate
	certData []byte
	keyData  []byte
}

// NewRotatingTLSCertificate creates TLS certificate provider
// object based on parameters specified in a Protobuf message for use
// with a TLS client. This Protobuf message is embedded in Buildbarn
// configuration files.
func NewRotatingTLSCertificate(certFile, keyFile string) *RotatingTLSCertificate {
	r := &RotatingTLSCertificate{
		certFile: certFile,
		keyFile:  keyFile,
	}

	return r
}

// GetCertificate provides the most recently obtained semantically valid tls.Certificate.
func (r *RotatingTLSCertificate) GetCertificate() *tls.Certificate {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.cert
}

// LoadCertificate from files on disk. Aborts if the files have not changed.
func (r *RotatingTLSCertificate) LoadCertificate() error {
	// Read certificate data from disk.
	certData, err := os.ReadFile(r.certFile)
	if err != nil {
		return util.StatusWrap(err, "Failed to read certificate file")
	}
	keyData, err := os.ReadFile(r.keyFile)
	if err != nil {
		return util.StatusWrap(err, "Failed to read private key file")
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// Skip updating if the certificate has not changed.
	if bytes.Equal(r.certData, certData) && bytes.Equal(r.keyData, keyData) {
		return nil
	}

	// Parse the PEM data into a certificate.
	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return util.StatusWrap(err, "Invalid certificate file or private key file")
	}

	// Update the certificate.
	r.cert = &cert
	r.certData = certData
	r.keyData = keyData

	return nil
}
