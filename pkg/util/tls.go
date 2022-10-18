package util

import (
	"crypto/tls"
	"crypto/x509"
	"sync"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/tls"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	cipherSuiteIDs = map[string]uint16{}

	tlsServerPrometheusMetrics sync.Once

	tlsServerCertificateNotBeforeTimeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "tls",
			Name:      "server_certificate_not_before_time_seconds",
			Help:      "The value of the \"Not Before\" field of the TLS server certificate.",
		},
		[]string{"dns_name"})
	tlsServerCertificateNotAfterTimeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "tls",
			Name:      "server_certificate_not_after_time_seconds",
			Help:      "The value of the \"Not After\" field of the TLS server certificate.",
		},
		[]string{"dns_name"})
)

func init() {
	// Initialize the map of cipher suite IDs based on the ciphers
	// supported by the Go TLS library.
	for _, cipherSuite := range tls.CipherSuites() {
		cipherSuiteIDs[cipherSuite.Name] = cipherSuite.ID
	}
}

func getBaseTLSConfig(cipherSuites []string) (*tls.Config, error) {
	tlsConfig := tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Resolve all provided cipher suite names.
	for _, cipherSuite := range cipherSuites {
		id, ok := cipherSuiteIDs[cipherSuite]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported cipher suite: %#v", cipherSuite)
		}
		tlsConfig.CipherSuites = append(tlsConfig.CipherSuites, id)
	}

	return &tlsConfig, nil
}

// NewTLSConfigFromClientConfiguration creates a TLS configuration
// object based on parameters specified in a Protobuf message for use
// with a TLS client. This Protobuf message is embedded in Buildbarn
// configuration files.
func NewTLSConfigFromClientConfiguration(configuration *configuration.ClientConfiguration) (*tls.Config, error) {
	if configuration == nil {
		return nil, nil
	}

	tlsConfig, err := getBaseTLSConfig(configuration.CipherSuites)
	if err != nil {
		return nil, err
	}
	tlsConfig.ServerName = configuration.ServerName

	if configuration.ClientCertificate != "" && configuration.ClientPrivateKey != "" {
		// Serve a client certificate when provided.
		cert, err := tls.X509KeyPair([]byte(configuration.ClientCertificate), []byte(configuration.ClientPrivateKey))
		if err != nil {
			return nil, StatusWrapWithCode(err, codes.InvalidArgument, "Invalid client certificate or private key")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if serverCAs := configuration.ServerCertificateAuthorities; serverCAs != "" {
		// Don't use the default root CA list. Use the ones
		// provided in the configuration instead.
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(serverCAs)) {
			return nil, status.Error(codes.InvalidArgument, "Invalid server certificate authorities")
		}
		tlsConfig.RootCAs = pool
	}

	return tlsConfig, nil
}

// NewTLSConfigFromServerConfiguration creates a TLS configuration
// object based on parameters specified in a Protobuf message for use
// with a TLS server. This Protobuf message is embedded in Buildbarn
// configuration files.
func NewTLSConfigFromServerConfiguration(configuration *configuration.ServerConfiguration) (*tls.Config, error) {
	tlsServerPrometheusMetrics.Do(func() {
		prometheus.MustRegister(tlsServerCertificateNotBeforeTimeSeconds)
		prometheus.MustRegister(tlsServerCertificateNotAfterTimeSeconds)
	})

	if configuration == nil {
		return nil, nil
	}

	tlsConfig, err := getBaseTLSConfig(configuration.CipherSuites)
	if err != nil {
		return nil, err
	}
	tlsConfig.ClientAuth = tls.RequestClientCert

	// Require the use of server-side certificates.
	cert, err := tls.X509KeyPair([]byte(configuration.ServerCertificate), []byte(configuration.ServerPrivateKey))
	if err != nil {
		return nil, StatusWrapWithCode(err, codes.InvalidArgument, "Invalid server certificate or private key")
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Expose Prometheus metrics on certificate expiration.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	for _, dnsName := range leaf.DNSNames {
		tlsServerCertificateNotBeforeTimeSeconds.WithLabelValues(dnsName).Set(float64(leaf.NotBefore.UnixNano()) / 1e9)
		tlsServerCertificateNotAfterTimeSeconds.WithLabelValues(dnsName).Set(float64(leaf.NotAfter.UnixNano()) / 1e9)
	}

	return tlsConfig, nil
}
