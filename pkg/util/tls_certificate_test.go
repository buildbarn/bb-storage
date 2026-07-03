package util_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// otherExampleCertificate and otherExamplePrivateKey form a second,
// distinct self-signed key pair, used to exercise certificate reloading.
// Like the primary pair, they are generated at test time so that no
// private key material needs to be embedded in source.
var otherExampleCertificate, otherExamplePrivateKey = mustGenerateExampleKeyPair()

func TestTLSCertificate(t *testing.T) {
	tempDir := t.TempDir()
	exampleCertFile := filepath.Join(tempDir, "example-cert.pem")
	exampleKeyFile := filepath.Join(tempDir, "example-key.pem")

	t.Run("LoadCertificate", func(t *testing.T) {
		os.WriteFile(exampleCertFile, []byte(exampleCertificate), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))

		cert := util.NewRotatingTLSCertificate(exampleCertFile, exampleKeyFile)

		err := cert.LoadCertificate()
		require.NoError(t, err)

		tlsCert := cert.GetCertificate()
		require.Len(t, tlsCert.Certificate, 1)
		// Second Get of certificate should b the same value
		require.Equal(t, cert.GetCertificate(), tlsCert)
	})

	t.Run("ReloadSameCertificate", func(t *testing.T) {
		os.WriteFile(exampleCertFile, []byte(exampleCertificate), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))

		cert := util.NewRotatingTLSCertificate(exampleCertFile, exampleKeyFile)

		err := cert.LoadCertificate()
		require.NoError(t, err)

		tlsCert := cert.GetCertificate()
		require.Len(t, tlsCert.Certificate, 1)

		err = cert.LoadCertificate()
		require.NoError(t, err)
		// Reloaded certificates should be the same value
		require.Equal(t, cert.GetCertificate(), tlsCert)
	})

	t.Run("ReloadSameCertificateAfterTouch", func(t *testing.T) {
		os.WriteFile(exampleCertFile, []byte(exampleCertificate), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))

		cert := util.NewRotatingTLSCertificate(exampleCertFile, exampleKeyFile)

		err := cert.LoadCertificate()
		require.NoError(t, err)

		tlsCert := cert.GetCertificate()
		require.Len(t, tlsCert.Certificate, 1)

		os.WriteFile(exampleCertFile, []byte(exampleCertificate), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))

		err = cert.LoadCertificate()
		require.NoError(t, err)
		// Reloaded certificates should be the same value
		require.Equal(t, cert.GetCertificate(), tlsCert)
	})

	t.Run("LoadChangedCertificate", func(t *testing.T) {
		os.WriteFile(exampleCertFile, []byte(exampleCertificate), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))

		cert := util.NewRotatingTLSCertificate(exampleCertFile, exampleKeyFile)

		err := cert.LoadCertificate()
		require.NoError(t, err)

		tlsCert := cert.GetCertificate()
		require.Len(t, tlsCert.Certificate, 1)

		os.WriteFile(exampleCertFile, []byte(otherExampleCertificate), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(otherExamplePrivateKey), os.FileMode(0o600))

		err = cert.LoadCertificate()
		require.NoError(t, err)
		// Certificates should be changed and not the same value
		require.NotEqual(t, cert.GetCertificate(), tlsCert)
	})

	t.Run("MismatchingCertificatePair", func(t *testing.T) {
		os.WriteFile(exampleCertFile, []byte(exampleCertFile), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(otherExamplePrivateKey), os.FileMode(0o600))

		cert := util.NewRotatingTLSCertificate(exampleCertFile, exampleKeyFile)

		err := cert.LoadCertificate()
		testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Invalid certificate file or private key file: tls: failed to find any PEM data in certificate input"), err)
	})

	t.Run("InvalidCertificate", func(t *testing.T) {
		os.WriteFile(exampleCertFile, []byte("This is an invalid certificate"), os.FileMode(0o600))
		os.WriteFile(exampleKeyFile, []byte(examplePrivateKey), os.FileMode(0o600))

		cert := util.NewRotatingTLSCertificate(exampleCertFile, exampleKeyFile)

		err := cert.LoadCertificate()
		testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Invalid certificate file or private key file: tls: failed to find any PEM data in certificate input"), err)
	})
}
