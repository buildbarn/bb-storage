package util

import (
	"crypto/x509"
	"encoding/pem"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Determine if the string containes the name of a PEM file or its contents (via importstr).
func IsPEMFile(s string) bool {
	return filepath.IsAbs(s) && strings.HasSuffix(strings.ToLower(s), ".pem")
}

// Parse raw bytes into x509 certificates.
func ParsePEMCerts(b []byte) ([]*x509.Certificate, error) {
	certs := []*x509.Certificate{}
	for len(b) != 0 {
		block, rem := pem.Decode(b)
		if block == nil {
			if len(certs) != 0 {
				break
			}
			return nil, status.Errorf(codes.FailedPrecondition, "Can't decode cert")
		}
		if block.Type != "CERTIFICATE" {
			if len(certs) != 0 {
				break
			}
			return nil, status.Errorf(codes.FailedPrecondition, "Wrong block type in cert file: %s", block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			if len(certs) != 0 {
				break
			}
			return nil, status.Errorf(codes.FailedPrecondition, "Can't parse cert: %v", err)
		}
		certs = append(certs, cert)
		b = rem
	}
	return certs, nil
}
