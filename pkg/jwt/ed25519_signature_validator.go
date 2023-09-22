package jwt

import (
	"crypto/ed25519"
)

type ed25519SignatureValidator struct {
	publicKey ed25519.PublicKey
}

// NewEd25519SignatureValidator creates a SignatureValidator that
// expects the signature of a JWT to use the Edwards-curve Digital
// Signature Algorithm (EdDSA), using Curve25519 as its elliptic curve
// and SHA-512 as a hashing algorithm.
//
// EdDSA uses asymmetrical cryptography, meaning that signing is
// performed using a private key, while verification only relies on a
// public key.
func NewEd25519SignatureValidator(publicKey ed25519.PublicKey) SignatureValidator {
	return &ed25519SignatureValidator{
		publicKey: publicKey,
	}
}

func (sv *ed25519SignatureValidator) ValidateSignature(algorithm, keyId, headerAndPayload string, signature []byte) bool {
	if algorithm != "EdDSA" {
		return false
	}
	return ed25519.Verify(sv.publicKey, []byte(headerAndPayload), signature)
}
