package main // spiffebundle

import (
	//"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/spiffe/go-spiffe/v2/spiffeid"

	"fmt"
	"os"
	"encoding/pem"
)

func main() {
	cert, key, err := newSPIFFECertAndKey("spiffe://acme.com/ns/project-id/sa/system-acct")
	if err != nil {
		fmt.Printf("can't create cert & key: %v\n", err)
	} else {
		certFile, err := os.Create("/tmp/cert.pem")
		if err != nil {
			fmt.Printf("can't create /tmp/cert.pem: %v\n", err)
			os.Exit(1)
		}
		defer certFile.Close()
		err = pem.Encode(certFile, &pem.Block{
			Type:   "CERTIFICATE",
			Bytes:  cert.Raw,
		})
		if err != nil {
			fmt.Printf("can't encode cert to /tmp/cert.pem: %v\n", err)
			os.Exit(1)
		}

		keyFile, err := os.Create("/tmp/key.pem")
		if err != nil {
			fmt.Printf("can't create /tmp/key.pem: %v\n", err)
			os.Exit(1)
		}
		defer keyFile.Close()
		b, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			fmt.Printf("can't marshal private key: %v\n", err)
			os.Exit(1)
		}
		err = pem.Encode(keyFile, &pem.Block{
			Type:   "PRIVATE KEY",
			Bytes:  b,
		})
		if err != nil {
			fmt.Printf("can't encode key to /tmp/key.pem: %v\n", err)
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Printf("can't extract SPIFFE ID: %v\n", err)
		os.Exit(1)
	}
	caCert, _, err := newSPIFFECaCertAndKey("spiffe://acme.com/ns/project-id/sa/system-acct")
	if err != nil {
		fmt.Printf("can't create CA cert & key: %v\n", err)
	} else {
		certFile, err := os.Create("/tmp/ca_cert.pem")
		if err != nil {
			fmt.Printf("can't create /tmp/ca_cert.pem: %v\n", err)
			os.Exit(1)
		}
		defer certFile.Close()
		err = pem.Encode(certFile, &pem.Block{
			Type:   "CERTIFICATE",
			Bytes:  caCert.Raw,
		})
		if err != nil {
			fmt.Printf("can't encode cert to /tmp/ca_cert.pem: %v\n", err)
			os.Exit(1)
		}
	}
}

func newSPIFFECertAndKey(spiffeID string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	u, err := url.Parse(spiffeID)
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

func newSPIFFECaCertAndKey(spiffeID string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	fmt.Printf("SPIFFE ID is %s\n", spiffeID)
	u, err := url.Parse(spiffeID)
	if err != nil {
		return nil, nil, err
	}
	fmt.Printf("URL is %v\n", u)
	td, err := spiffeid.TrustDomainFromString(spiffeID)
	if err != nil {
		return nil, nil, fmt.Errorf("can't create trust domain: %v", err)
	}
        u, err = url.Parse(td.IDString())
        if err != nil {
                return nil, nil, err
        }
	fmt.Printf("URL is %v\n", u)

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
