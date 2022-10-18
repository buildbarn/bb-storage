package jwt_test

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/stretchr/testify/require"
)

func TestECDSASHASignatureGenerator(t *testing.T) {
	t.Run("ES256", func(t *testing.T) {
		// Create a signature generator.
		block, _ := pem.Decode([]byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIBEf3U53imN4pfQBPtGT1TG+SZfkiSDKITr/SMuscXVDoAoGCCqGSM49
AwEHoUQDQgAEkx7qtWaVCs8Xlqwnfb5X64NvE7uuxjv5JcjJhfkwdjrEms5bpIy/
f2EJfEoVNO/YidkVY+J35v8vQoAMS4rRGA==
-----END EC PRIVATE KEY-----`))
		require.NotNil(t, block)
		key, err := x509.ParseECPrivateKey(block.Bytes)
		require.NoError(t, err)
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
		require.True(t, signatureValidator.ValidateSignature("ES256", headerAndPayload, signature))
	})
}
