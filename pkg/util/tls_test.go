package util_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/tls"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// mustGenerateExampleKeyPair generates a self-signed certificate and its
// matching private key at test time, returning them as PEM encoded
// strings. Generating key material dynamically avoids embedding private
// keys in source code.
func mustGenerateExampleKeyPair() (certificate, privateKey string) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	now := time.Now()
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "example.com"},
		DNSNames:              []string{"example.com"},
		NotBefore:             now.Add(-10 * time.Minute),
		NotAfter:              now.Add(time.Hour),
		BasicConstraintsValid: true,
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER}))
}

var exampleCertificate, examplePrivateKey = mustGenerateExampleKeyPair()

func TestTLSConfigFromClientConfiguration(t *testing.T) {
	tempDir := t.TempDir()
	exampleCertFile := filepath.Join(tempDir, "example-cert.pem")
	exampleKeyFile := filepath.Join(tempDir, "example-key.pem")
	exampleInvalidCertFile := filepath.Join(tempDir, "example-invalid-cert.pem")
	os.WriteFile(exampleCertFile, []byte(exampleCertificate), os.FileMode(0o600))
	os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))
	os.WriteFile(exampleInvalidCertFile, []byte("This is an invalid certificate"), os.FileMode(0o600))

	t.Run("Disabled", func(t *testing.T) {
		// When the TLS configuration is nil, TLS should be left
		// disabled.
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(nil)
		require.NoError(t, err)
		require.Nil(t, tlsConfig)
	})

	t.Run("Default", func(t *testing.T) {
		// The default configuration should enforce the use of
		// TLS 1.2 or higher.
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{},
		)
		require.NoError(t, err)
		require.Equal(t, &tls.Config{
			MinVersion: tls.VersionTLS12,
		}, tlsConfig)
	})

	t.Run("ClientCertificateInline", func(t *testing.T) {
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ClientKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Inline_{
						Inline: &configuration.X509KeyPair_Inline{
							Certificate: exampleCertificate,
							PrivateKey:  examplePrivateKey,
						},
					},
				},
			},
		)
		require.NoError(t, err)
		cert, err := tlsConfig.GetClientCertificate(nil)
		require.NoError(t, err)
		require.Len(t, cert.Certificate, 1)
	})

	t.Run("ClientCertificateFiles", func(t *testing.T) {
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ClientKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Files_{
						Files: &configuration.X509KeyPair_Files{
							CertificatePath: exampleCertFile,
							PrivateKeyPath:  exampleKeyFile,
							RefreshInterval: durationpb.New(time.Hour),
						},
					},
				},
			},
		)
		require.NoError(t, err)
		cert, err := tlsConfig.GetClientCertificate(nil)
		require.NoError(t, err)
		require.Len(t, cert.Certificate, 1)
	})

	t.Run("InvalidClientCertificateInline", func(t *testing.T) {
		_, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ClientKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Inline_{
						Inline: &configuration.X509KeyPair_Inline{
							Certificate: "This is an invalid certificate",
							PrivateKey:  examplePrivateKey,
						},
					},
				},
			},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to configure client TLS: Invalid certificate or private key: tls: failed to find any PEM data in certificate input"), err)
	})

	t.Run("InvalidClientCertificateFiles", func(t *testing.T) {
		_, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ClientKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Files_{
						Files: &configuration.X509KeyPair_Files{
							CertificatePath: exampleInvalidCertFile,
							PrivateKeyPath:  exampleKeyFile,
							RefreshInterval: durationpb.New(time.Hour),
						},
					},
				},
			},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to configure client TLS: Failed to initialize certificate: Invalid certificate file or private key file: tls: failed to find any PEM data in certificate input"), err)
	})

	t.Run("ServerCertificateAuthorities", func(t *testing.T) {
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ServerCertificateAuthorities: exampleCertificate,
			},
		)
		require.NoError(t, err)
		require.Len(t, tlsConfig.RootCAs.Subjects(), 1)
	})

	t.Run("InvalidServerCertificateAuthorities", func(t *testing.T) {
		// Because CertPool.AppendCertsFromPEM() does not return
		// a rich error message, we have no choice but to return
		// a simple error message in case of CA parsing failures.
		// https://github.com/golang/go/issues/23711#issuecomment-363322424
		_, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ServerCertificateAuthorities: "This is an invalid certificate",
			},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid server certificate authorities"), err)
	})

	t.Run("CustomCipherSuites", func(t *testing.T) {
		// Custom cipher suites should be respected.
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				CipherSuites: []string{
					"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
					"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
					"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
					"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
				},
			},
		)
		require.NoError(t, err)
		require.Equal(t, &tls.Config{
			MinVersion: tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
		}, tlsConfig)
	})

	t.Run("InvalidCipherSuite", func(t *testing.T) {
		_, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				CipherSuites: []string{
					"TLS_ECDHE_ECDSA_WITH_AES_257_GCM_SHA385",
				},
			},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unsupported cipher suite: \"TLS_ECDHE_ECDSA_WITH_AES_257_GCM_SHA385\""), err)
	})

	t.Run("ServerName", func(t *testing.T) {
		tlsConfig, err := util.NewTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ServerName: "example.com",
			},
		)
		require.NoError(t, err)
		require.Equal(t, &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: "example.com",
		}, tlsConfig)
	})
}

func TestTLSConfigFromServerConfiguration(t *testing.T) {
	tempDir := t.TempDir()
	exampleCertFile := filepath.Join(tempDir, "example-cert.pem")
	exampleKeyFile := filepath.Join(tempDir, "example-key.pem")
	exampleInvalidCertFile := filepath.Join(tempDir, "example-invalid-cert.pem")
	os.WriteFile(exampleCertFile, []byte(exampleCertificate), os.FileMode(0o600))
	os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))
	os.WriteFile(exampleInvalidCertFile, []byte("This is an invalid certificate"), os.FileMode(0o600))

	t.Run("Disabled", func(t *testing.T) {
		// When the TLS configuration is nil, TLS should be left
		// disabled.
		tlsConfig, err := util.NewTLSConfigFromServerConfiguration(nil, false)
		require.NoError(t, err)
		require.Nil(t, tlsConfig)
	})

	t.Run("DefaultCertInline", func(t *testing.T) {
		// The default configuration should enforce the use of
		// TLS 1.2 or higher.
		tlsConfig, err := util.NewTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Inline_{
						Inline: &configuration.X509KeyPair_Inline{
							Certificate: exampleCertificate,
							PrivateKey:  examplePrivateKey,
						},
					},
				},
			},
			/* requestClientCertificate = */ true,
		)
		require.NoError(t, err)
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Len(t, cert.Certificate, 1)
		tlsConfig.GetCertificate = nil
		require.Equal(t, &tls.Config{
			MinVersion: tls.VersionTLS12,
			ClientAuth: tls.RequestClientCert,
		}, tlsConfig)
	})

	t.Run("DefaultCertFiles", func(t *testing.T) {
		// The default configuration should enforce the use of
		// TLS 1.2 or higher.
		tlsConfig, err := util.NewTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Files_{
						Files: &configuration.X509KeyPair_Files{
							CertificatePath: exampleCertFile,
							PrivateKeyPath:  exampleKeyFile,
							RefreshInterval: durationpb.New(time.Hour),
						},
					},
				},
			},
			/* requestClientCertificate = */ true,
		)
		require.NoError(t, err)
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Len(t, cert.Certificate, 1)
		tlsConfig.GetCertificate = nil
		require.Equal(t, &tls.Config{
			MinVersion: tls.VersionTLS12,
			ClientAuth: tls.RequestClientCert,
		}, tlsConfig)
	})

	t.Run("InvalidServerCertificateInline", func(t *testing.T) {
		_, err := util.NewTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Inline_{
						Inline: &configuration.X509KeyPair_Inline{
							Certificate: "This is an invalid certificate",
							PrivateKey:  examplePrivateKey,
						},
					},
				},
			},
			/* requestClientCertificate = */ false,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to configure server TLS: Invalid certificate or private key: tls: failed to find any PEM data in certificate input"), err)
	})

	t.Run("InvalidServerCertificateFiles", func(t *testing.T) {
		_, err := util.NewTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Files_{
						Files: &configuration.X509KeyPair_Files{
							CertificatePath: exampleInvalidCertFile,
							PrivateKeyPath:  exampleKeyFile,
							RefreshInterval: durationpb.New(time.Hour),
						},
					},
				},
			},
			/* requestClientCertificate = */ false,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to configure server TLS: Failed to initialize certificate: Invalid certificate file or private key file: tls: failed to find any PEM data in certificate input"), err)
	})

	t.Run("MissingServerCertificateFiles", func(t *testing.T) {
		_, err := util.NewTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Files_{
						Files: &configuration.X509KeyPair_Files{
							CertificatePath: "/missing-cert.pem",
							PrivateKeyPath:  "/missing-key.pem",
							RefreshInterval: durationpb.New(time.Hour),
						},
					},
				},
			},
			/* requestClientCertificate = */ false,
		)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to configure server TLS: Failed to initialize certificate: Failed to read certificate file: open /missing-cert.pem: "), err)
	})

	t.Run("CustomCipherSuites", func(t *testing.T) {
		// Custom cipher suites should be respected.
		tlsConfig, err := util.NewTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Inline_{
						Inline: &configuration.X509KeyPair_Inline{
							Certificate: exampleCertificate,
							PrivateKey:  examplePrivateKey,
						},
					},
				},
				CipherSuites: []string{
					"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
					"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
					"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
					"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
				},
			},
			/* requestClientCertificate = */ false,
		)
		require.NoError(t, err)
		cert, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Len(t, cert.Certificate, 1)
		tlsConfig.GetCertificate = nil
		require.Equal(t, &tls.Config{
			MinVersion: tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
		}, tlsConfig)
	})

	t.Run("InvalidCipherSuite", func(t *testing.T) {
		_, err := util.NewTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair: &configuration.X509KeyPair{
					KeyPair: &configuration.X509KeyPair_Inline_{
						Inline: &configuration.X509KeyPair_Inline{
							Certificate: exampleCertificate,
							PrivateKey:  examplePrivateKey,
						},
					},
				},
				CipherSuites: []string{
					"TLS_ECDHE_ECDSA_WITH_AES_257_GCM_SHA385",
				},
			},
			/* requestClientCertificate = */ false,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unsupported cipher suite: \"TLS_ECDHE_ECDSA_WITH_AES_257_GCM_SHA385\""), err)
	})
}
