package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func newSPIFFECertAndKey(spiffeId string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	u, err := url.Parse(spiffeId)
	if err != nil {
		return nil, nil, err
	}
	now := clock.SystemClock.Now()
	cert := &x509.Certificate{
		Subject: pkix.Name{
			Country:      []string{"US"},
			Organization: []string{"Acme Corp."},
		},
		NotBefore:             now,
		NotAfter:              now.Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment |
			               x509.KeyUsageKeyAgreement |
			               x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		URIs:                  []*url.URL{u},
	}
	cert, key, err := signCert(cert)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func newSPIFFECaCertAndKey(td spiffeid.TrustDomain) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	u, err := url.Parse(td.IDString())
	if err != nil {
		return nil, nil, err
	}
	now := clock.SystemClock.Now()
	name := pkix.Name{
		Country:      []string{"US"},
		Organization: []string{"Acme Corp."},
	}
	cert := &x509.Certificate{
		Subject:               name,
		Issuer:                name,
		IsCA:                  true,
		NotBefore:             now,
		NotAfter:              now.Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		URIs:                  []*url.URL{u},
	}
	cert, key, err := signCert(cert)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func signCert(req *x509.Certificate) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	publicKey := privateKey.Public()
	req.SerialNumber, _ = rand.Int(rand.Reader, big.NewInt(666))

	certData, err := x509.CreateCertificate(rand.Reader, req, req, publicKey, privateKey)  // self-sign
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(certData)
	if err != nil {
		return nil, nil, err
	}
	return cert, privateKey, nil
}

func MakeCaPemFile(t *testing.T, spiffeId string) (string) {
	td, err := spiffeid.TrustDomainFromString(spiffeId)
	if err != nil {
		t.Errorf("can't extract SPIFFE ID: %v", err)
	}
	caCert, _, err := newSPIFFECaCertAndKey(td)
	if err != nil {
		t.Errorf("can't create CA cert & key: %v", err)
	}
	certPath := t.TempDir() + "/ca_cert.pem"
	certFile, err := os.Create(certPath)
	if err != nil {
		t.Errorf("can't create %s: %v", certPath, err)
	}
	defer certFile.Close()
	err = pem.Encode(certFile, &pem.Block{
		Type:   "CERTIFICATE",
		Bytes:  caCert.Raw,
	})
	if err != nil {
		t.Errorf("can't encode cert to %s: %v", certPath, err)
	}
	return certPath
}

func MakeCertAndKeyPemFiles(t *testing.T, spiffeId string) (string, string) {
	caCert, key, err := newSPIFFECertAndKey(spiffeId)
	if err != nil {
		t.Errorf("can't create CA cert & key: %v", err)
	}
	tmp := t.TempDir()
	certPath := tmp + "/ca_cert.pem"
	certFile, err := os.Create(certPath)
	if err != nil {
		t.Errorf("can't create %s: %v", certPath, err)
	}
	defer certFile.Close()
	err = pem.Encode(certFile, &pem.Block{
		Type:   "CERTIFICATE",
		Bytes:  caCert.Raw,
	})
	if err != nil {
		t.Errorf("can't encode cert to %s: %v", certPath, err)
	}

	keyPath := tmp + "/key.pem"
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Errorf("can't create %s: %v", keyPath, err)
	}
	defer keyFile.Close()
	b, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Errorf("can't marshal private key: %v", err)
	}
	err = pem.Encode(keyFile, &pem.Block{
		Type:   "PRIVATE KEY",
		Bytes:  b,
	})
	if err != nil {
		t.Errorf("can't encode key to %s: %v\n", keyPath, err)
	}

	return certPath, keyPath
}
