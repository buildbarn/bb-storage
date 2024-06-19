package jwt_test

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/stretchr/testify/require"
)

func TestEd25519SignatureGenerator(t *testing.T) {
	// Create a signature generator.
	block, _ := pem.Decode([]byte(`-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJFl+ugysbBc60+O+IIFLSJL0TYtV1iW9W9YQ9t2l4MN
-----END PRIVATE KEY-----`))
	require.NotNil(t, block)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	require.NoError(t, err)
	privateKey := key.(ed25519.PrivateKey)
	signatureGenerator := jwt.NewEd25519SignatureGenerator(privateKey)
	require.Equal(t, "EdDSA", signatureGenerator.GetAlgorithm())

	// Sign a token.
	headerAndPayload := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"
	signature, err := signatureGenerator.GenerateSignature(headerAndPayload)
	require.NoError(t, err)

	// Ensure that the generated signature is valid.
	signatureValidator := jwt.NewEd25519SignatureValidator(privateKey.Public().(ed25519.PublicKey))
	require.NoError(t, err)
	require.True(t, signatureValidator.ValidateSignature("EdDSA", nil, headerAndPayload, signature))
}
