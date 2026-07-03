package jwt_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/stretchr/testify/require"
)

func TestECDSASHASignatureGenerator(t *testing.T) {
	t.Run("ES256", func(t *testing.T) {
		// Generate an ECDSA key pair at test time, so that no
		// private key material needs to be embedded in source.
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		// Create a signature generator.
		signatureGenerator, err := jwt.NewECDSASHASignatureGenerator(key, random.CryptoThreadSafeGenerator)
		require.NoError(t, err)
		require.Equal(t, "ES256", signatureGenerator.GetAlgorithm())

		// Sign a token.
		headerAndPayload := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"
		signature, err := signatureGenerator.GenerateSignature(headerAndPayload)
		require.NoError(t, err)

		// Ensure that the generated signature is valid.
		signatureValidator, err := jwt.NewECDSASHASignatureValidator(&key.PublicKey)
		require.NoError(t, err)
		require.True(t, signatureValidator.ValidateSignature("ES256", nil, headerAndPayload, signature))
	})
}
