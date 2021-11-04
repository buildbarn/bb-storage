package jwt

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
)

type hmacSHASignatureValidator struct {
	key []byte
}

// NewHMACSHASignatureValidator creates a SignatureValidator that
// expects the signature of a JWT to use Hash-based Message
// Authentication Code (HMAC), using SHA-256, SHA-384 or SHA-512 as a
// hashing algorithm.
//
// HMAC uses symmetric cryptography, meaning that the key used to sign a
// JWT is the same as the one used to validate it. There is no
// distinction between public and private keys, which may not be
// desirable from a security point of view.
func NewHMACSHASignatureValidator(key []byte) SignatureValidator {
	return &hmacSHASignatureValidator{
		key: key,
	}
}

func (sv *hmacSHASignatureValidator) ValidateSignature(algorithm, headerAndPayload string, signature []byte) bool {
	// Determine the hashing function that was used to create the
	// signature.
	var hashFunc func() hash.Hash
	switch algorithm {
	case "HS256":
		hashFunc = sha256.New
	case "HS384":
		hashFunc = sha512.New384
	case "HS512":
		hashFunc = sha512.New
	default:
		return false
	}
	hasher := hmac.New(hashFunc, sv.key)
	hasher.Write([]byte(headerAndPayload))
	return hmac.Equal(hasher.Sum(nil), signature)
}
