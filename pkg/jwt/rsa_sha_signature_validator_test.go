package jwt_test

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/stretchr/testify/require"
)

func TestRSASHASignatureValidator(t *testing.T) {
	// Generate an RSA key pair at test time, so that no private key
	// material needs to be embedded in source.
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	signatureValidator := jwt.NewRSASHASignatureValidator(&privateKey.PublicKey)

	// Any JWT header and payload. Its contents are irrelevant, as the
	// validator only inspects the algorithm that is passed in
	// explicitly.
	headerAndPayload := "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0"

	// Algorithm "ES256" uses ECDSA, not RSA. Validation should fail
	// regardless of the signature that is provided.
	require.False(t, signatureValidator.ValidateSignature(
		"ES256",
		/* keyID = */ nil,
		headerAndPayload,
		[]byte{0x00},
	))

	for _, tc := range []struct {
		algorithm string
		hashType  crypto.Hash
		newHasher func() hash.Hash
		pss       bool
	}{
		// PKCS#1 v1.5 padding.
		{"RS256", crypto.SHA256, sha256.New, false},
		{"RS384", crypto.SHA384, sha512.New384, false},
		{"RS512", crypto.SHA512, sha512.New, false},
		// PSS padding, whose signatures are randomized via salt.
		{"PS256", crypto.SHA256, sha256.New, true},
		{"PS384", crypto.SHA384, sha512.New384, true},
		{"PS512", crypto.SHA512, sha512.New, true},
	} {
		t.Run(tc.algorithm, func(t *testing.T) {
			hasher := tc.newHasher()
			hasher.Write([]byte(headerAndPayload))
			digest := hasher.Sum(nil)

			var validSignature []byte
			var err error
			if tc.pss {
				validSignature, err = rsa.SignPSS(rand.Reader, privateKey, tc.hashType, digest,
					&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: tc.hashType})
			} else {
				validSignature, err = rsa.SignPKCS1v15(rand.Reader, privateKey, tc.hashType, digest)
			}
			require.NoError(t, err)

			// A valid signature should be accepted.
			require.True(t, signatureValidator.ValidateSignature(
				tc.algorithm,
				/* keyID = */ nil,
				headerAndPayload,
				validSignature,
			))

			// Flipping a byte of the signature should cause
			// validation to fail.
			invalidSignature := bytes.Clone(validSignature)
			invalidSignature[0] ^= 0xff
			require.False(t, signatureValidator.ValidateSignature(
				tc.algorithm,
				/* keyID = */ nil,
				headerAndPayload,
				invalidSignature,
			))
		})
	}
}
