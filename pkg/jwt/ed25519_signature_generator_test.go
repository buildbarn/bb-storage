package jwt_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/stretchr/testify/require"
)

func TestEd25519SignatureGenerator(t *testing.T) {
	// Generate an Ed25519 key pair at test time, so that no private
	// key material needs to be embedded in source.
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// Create a signature generator.
	signatureGenerator := jwt.NewEd25519SignatureGenerator(privateKey)
	require.Equal(t, "EdDSA", signatureGenerator.GetAlgorithm())

	// Sign a token.
	headerAndPayload := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"
	signature, err := signatureGenerator.GenerateSignature(headerAndPayload)
	require.NoError(t, err)

	// Ensure that the generated signature is valid.
	signatureValidator := jwt.NewEd25519SignatureValidator(publicKey)
	require.True(t, signatureValidator.ValidateSignature("EdDSA", nil, headerAndPayload, signature))
}
