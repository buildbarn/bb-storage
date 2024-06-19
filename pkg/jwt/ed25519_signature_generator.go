package jwt

import (
	"crypto/ed25519"
)

type ed25519SignatureGenerator struct {
	privateKey ed25519.PrivateKey
}

// NewEd25519SignatureGenerator creates a SignatureGenerator that can
// sign a JWT using the Edwards-curve Digital Signature Algorithm
// (EdDSA), using Curve25519 as its elliptic curve and SHA-512 as a
// hashing algorithm.
//
// EdDSA uses asymmetrical cryptography, meaning that signing is
// performed using a private key, while verification only relies on a
// public key.
func NewEd25519SignatureGenerator(privateKey ed25519.PrivateKey) SignatureGenerator {
	return ed25519SignatureGenerator{
		privateKey: privateKey,
	}
}

func (sc ed25519SignatureGenerator) GetAlgorithm() string {
	return "EdDSA"
}

func (sc ed25519SignatureGenerator) GenerateSignature(headerAndPayload string) ([]byte, error) {
	return ed25519.Sign(sc.privateKey, []byte(headerAndPayload)), nil
}
