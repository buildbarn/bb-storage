package bb_tls_test

import (
	"crypto/tls"
	"io/ioutil"
	"testing"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/tls"
	grpc_cfg "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	spiffe "github.com/buildbarn/bb-storage/pkg/proto/configuration/spiffe"

	"github.com/buildbarn/bb-storage/pkg/bb_tls"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMTLSConfigFromClientConfiguration(t *testing.T) {
	t.Run("SpiffeClientCertificate", func(t *testing.T) {
		spiffeCertFile, spiffeKeyFile := testutil.MakeCertAndKeyPemFiles(t, "spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
		spiffeCACertsFile := testutil.MakeCaPemFile(t, "spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
		tlsConfig, err := bb_tls.NewMTLSConfigFromClientConfiguration(
			&configuration.ClientConfiguration{
				ClientKeyPair:                &configuration.X509KeyPair{
									KeyPair: &configuration.X509KeyPair_Files_{
										Files: &configuration.X509KeyPair_Files{
											CertificatePath: spiffeCertFile,
											PrivateKeyPath:  spiffeKeyFile,
										},
								        },
				                              },
				ServerCertificateAuthorities: spiffeCACertsFile,
			})
		require.NoError(t, err)
		cb, err := ioutil.ReadFile(spiffeCertFile)
		if err != nil {
			t.Errorf("can't read %s: %v", spiffeCertFile, err)
		}
		kb, err := ioutil.ReadFile(spiffeKeyFile)
		if err != nil {
			t.Errorf("can't read %s: %v", spiffeKeyFile, err)
		}
		cert, err := tls.X509KeyPair(cb, kb)
		if err != nil {
			t.Errorf("can't parse keypair: %v", err)
		}
		c, err := tlsConfig.GetClientCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, cert, *c)
		require.Equal(t, tls.VersionTLS12, int(tlsConfig.MinVersion))
	})
}

func TestMTLSConfigFromServerConfiguration(t *testing.T) {
	t.Run("SpiffeServerCertificate", func(t *testing.T) {
		spiffeCertFile, spiffeKeyFile := testutil.MakeCertAndKeyPemFiles(t, "spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
		spiffeCACertsFile := testutil.MakeCaPemFile(t, "spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
		tlsConfig, err := bb_tls.NewMTLSConfigFromServerConfiguration(
			&configuration.ServerConfiguration{
				ServerKeyPair:       &configuration.X509KeyPair{
							KeyPair: &configuration.X509KeyPair_Files_{
								Files: &configuration.X509KeyPair_Files{
									CertificatePath: spiffeCertFile,
									PrivateKeyPath:  spiffeKeyFile,
								},
							},
				                     },
				ValidateClientCerts: true,
			},
			&grpc_cfg.AuthenticationPolicy{
				Policy: &grpc_cfg.AuthenticationPolicy_TlsClientCertificate{
					TlsClientCertificate: &grpc_cfg.TLSClientCertificateAuthenticationPolicy{
						ClientCertificateAuthorities: spiffeCACertsFile,
						Spiffe:                       &spiffe.SubjectMatcher{
							AllowedSpiffeIds:     map[string]string{"*": "*"},
						},
					},
				},
			})
		cb, err := ioutil.ReadFile(spiffeCertFile)
		if err != nil {
			t.Errorf("can't read %s: %v", spiffeCertFile, err)
		}
		kb, err := ioutil.ReadFile(spiffeKeyFile)
		if err != nil {
			t.Errorf("can't read %s: %v", spiffeKeyFile, err)
		}
		cert, err := tls.X509KeyPair(cb, kb)
		if err != nil {
			t.Errorf("can't parse keypair: %v", err)
		}
		c, err := tlsConfig.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, cert, *c)
		require.Equal(t, tls.VersionTLS12, int(tlsConfig.MinVersion))
		require.Equal(t, tls.RequireAnyClientCert, tlsConfig.ClientAuth)
	})
}
