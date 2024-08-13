package util

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"sync"
	"time"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/tls"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	cipherSuiteIDs = map[string]uint16{}

	tlsPrometheusMetrics sync.Once

	tlsCertificateUsageClient = "client"
	tlsCertificateUsageServer = "server"

	tlsCertificateNotBeforeTimeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "tls",
			Name:      "certificate_not_before_time_seconds",
			Help:      "The value of the \"Not Before\" field of the TLS certificate.",
		},
		[]string{"dns_name", "uri", "ip_address", "certificate_usage"})
	tlsCertificateNotAfterTimeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "tls",
			Name:      "certificate_not_after_time_seconds",
			Help:      "The value of the \"Not After\" field of the TLS certificate.",
		},
		[]string{"dns_name", "uri", "ip_address", "certificate_usage"})
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

func updateTLSCertificateExpiry(cert *tls.Certificate, certificateUsage string) error {
	// Expose Prometheus metrics on certificate expiration.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	for _, dnsName := range leaf.DNSNames {
		tlsCertificateNotBeforeTimeSeconds.WithLabelValues(dnsName, "", "", certificateUsage).Set(float64(leaf.NotBefore.UnixNano()) / 1e9)
		tlsCertificateNotAfterTimeSeconds.WithLabelValues(dnsName, "", "", certificateUsage).Set(float64(leaf.NotAfter.UnixNano()) / 1e9)
	}
	for _, uri := range leaf.URIs {
		uriStr := uri.String()
		tlsCertificateNotBeforeTimeSeconds.WithLabelValues("", uriStr, "", certificateUsage).Set(float64(leaf.NotBefore.UnixNano()) / 1e9)
		tlsCertificateNotAfterTimeSeconds.WithLabelValues("", uriStr, "", certificateUsage).Set(float64(leaf.NotAfter.UnixNano()) / 1e9)
	}
	for _, ip := range leaf.IPAddresses {
		ipStr := ip.String()
		tlsCertificateNotBeforeTimeSeconds.WithLabelValues("", "", ipStr, certificateUsage).Set(float64(leaf.NotBefore.UnixNano()) / 1e9)
		tlsCertificateNotAfterTimeSeconds.WithLabelValues("", "", ipStr, certificateUsage).Set(float64(leaf.NotAfter.UnixNano()) / 1e9)
	}
	return nil
}

func refreshTLSCertOnInterval(cert *RotatingTLSCertificate, refreshInterval *durationpb.Duration, certificateUsage string) error {
	if err := cert.LoadCertificate(); err != nil {
		return err
	}
	updateTLSCertificateExpiry(cert.GetCertificate(), certificateUsage)

	if err := refreshInterval.CheckValid(); err != nil {
		return StatusWrap(err, "Failed to parse refresh interval")
	}

	// TODO: Run this as part of the program.Group, so that it gets
	// cleaned up upon shutdown.
	go func() {
		t := time.NewTicker(refreshInterval.AsDuration())
		for {
			<-t.C
			if err := cert.LoadCertificate(); err != nil {
				// Don't fail or break the existing TLS creds, since it is likely still functioning.
				// Hope that at the next refresh interval the certificate may be valid.
				log.Printf("Failed to reload %s certificate: %v", certificateUsage, err)
			} else {
				// Update expiry when we load a new certificate
				updateTLSCertificateExpiry(cert.GetCertificate(), certificateUsage)
			}
		}
	}()

	return nil
}

func registerTLSCertificate(tlsKeyPair *pb.X509KeyPair, certificateUsage string) (func() *tls.Certificate, error) {
	switch keyPair := tlsKeyPair.KeyPair.(type) {
	case *pb.X509KeyPair_Inline_:
		cert, err := tls.X509KeyPair([]byte(keyPair.Inline.Certificate), []byte(keyPair.Inline.PrivateKey))
		if err != nil {
			return nil, StatusWrap(err, "Invalid certificate or private key")
		}
		updateTLSCertificateExpiry(&cert, certificateUsage)
		return func() *tls.Certificate { return &cert }, nil

	case *pb.X509KeyPair_Files_:
		cert := NewRotatingTLSCertificate(keyPair.Files.CertificatePath, keyPair.Files.PrivateKeyPath)
		if err := refreshTLSCertOnInterval(cert, keyPair.Files.RefreshInterval, certificateUsage); err != nil {
			return nil, StatusWrap(err, "Failed to initialize certificate")
		}
		return cert.GetCertificate, nil
	default:
		return nil, errors.New("unexpected key-pair type")
	}
}

// NewTLSConfigFromClientConfiguration creates a TLS configuration
// object based on parameters specified in a Protobuf message for use
// with a TLS client. This Protobuf message is embedded in Buildbarn
// configuration files.
func NewTLSConfigFromClientConfiguration(configuration *pb.ClientConfiguration) (*tls.Config, error) {
	tlsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(tlsCertificateNotAfterTimeSeconds)
		prometheus.MustRegister(tlsCertificateNotBeforeTimeSeconds)
	})

	if configuration == nil {
		return nil, nil
	}

	tlsConfig, err := getBaseTLSConfig(configuration.CipherSuites)
	if err != nil {
		return nil, err
	}
	tlsConfig.ServerName = configuration.ServerName

	// No client TLS is used when this is unset.
	if configuration.ClientKeyPair != nil {
		getLatestCert, err := registerTLSCertificate(configuration.ClientKeyPair, tlsCertificateUsageClient)
		if err != nil {
			return nil, StatusWrapWithCode(err, codes.InvalidArgument, "Failed to configure client TLS")
		}
		tlsConfig.GetClientCertificate = func(chi *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return getLatestCert(), nil
		}
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
func NewTLSConfigFromServerConfiguration(configuration *pb.ServerConfiguration, requestClientCertificate bool) (*tls.Config, error) {
	tlsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(tlsCertificateNotAfterTimeSeconds)
		prometheus.MustRegister(tlsCertificateNotBeforeTimeSeconds)
	})

	if configuration == nil {
		return nil, nil
	}

	tlsConfig, err := getBaseTLSConfig(configuration.CipherSuites)
	if err != nil {
		return nil, err
	}
	if requestClientCertificate {
		tlsConfig.ClientAuth = tls.RequestClientCert
	}

	if configuration.ServerKeyPair == nil {
		return nil, StatusWrapWithCode(err, codes.InvalidArgument, "Missing server_key_pair configuration")
	}

	// Require the use of server-side certificates.
	getLatestCert, err := registerTLSCertificate(configuration.ServerKeyPair, tlsCertificateUsageServer)
	if err != nil {
		return nil, StatusWrapWithCode(err, codes.InvalidArgument, "Failed to configure server TLS")
	}
	tlsConfig.GetCertificate = func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
		return getLatestCert(), nil
	}

	return tlsConfig, nil
}
