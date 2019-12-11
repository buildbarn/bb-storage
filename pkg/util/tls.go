package util

import (
	"crypto/tls"
	"crypto/x509"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/tls"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewTLSConfigFromClientConfiguration creates a TLS configuration
// object based on parameters specified in a Protobuf message for use
// with a TLS client. This Protobuf message is embedded in Buildbarn
// configuration files.
func NewTLSConfigFromClientConfiguration(configuration *configuration.TLSClientConfiguration) (*tls.Config, error) {
	if configuration == nil {
		return nil, nil
	}

	var tlsConfig tls.Config
	if configuration.ClientCertificate != "" && configuration.ClientPrivateKey != "" {
		// Serve a client certificate when provided.
		cert, err := tls.X509KeyPair([]byte(configuration.ClientCertificate), []byte(configuration.ClientPrivateKey))
		if err != nil {
			return nil, StatusWrap(err, "Failed to load X509 key pair")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if serverCAs := configuration.ServerCertificateAuthorities; serverCAs != "" {
		// Don't use the default root CA list. Use the ones
		// provided in the configuration instead.
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(serverCAs)) {
			return nil, status.Error(codes.InvalidArgument, "Failed to parse server certificate authorities")
		}
		tlsConfig.RootCAs = pool
	}

	return &tlsConfig, nil
}

// NewTLSConfigFromServerConfiguration creates a TLS configuration
// object based on parameters specified in a Protobuf message for use
// with a TLS server. This Protobuf message is embedded in Buildbarn
// configuration files.
func NewTLSConfigFromServerConfiguration(configuration *configuration.TLSServerConfiguration) (*tls.Config, error) {
	if configuration == nil {
		return nil, nil
	}

	tlsConfig := tls.Config{
		ClientAuth: tls.RequestClientCert,
	}

	// Require the use of server-side certificates.
	cert, err := tls.X509KeyPair([]byte(configuration.ServerCertificate), []byte(configuration.ServerPrivateKey))
	if err != nil {
		return nil, StatusWrap(err, "Failed to load X509 key pair")
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	return &tlsConfig, nil
}
