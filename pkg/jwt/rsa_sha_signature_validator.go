package jwt

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
)

type rsaSHASignatureValidator struct {
	key *rsa.PublicKey
}

// NewRSASHASignatureValidator creates a SignatureValidator that expects
// the signature of a JWT to use the Rivest-Shamir-Adleman (RSA)
// cryptosystem, using SHA-256, SHA-384 or SHA-512 as a hashing
// algorithm. Both PKCS#1 v1.5 (RS256/RS384/RS512) and PSS
// (PS256/PS384/PS512) padding schemes are supported.
//
// RSA uses asymmetrical cryptography, meaning that signing is performed
// using a private key, while verification only relies on a public key.
// Signatures tend to be a lot larger than those created by ECDSA and
// EdDSA.
func NewRSASHASignatureValidator(key *rsa.PublicKey) SignatureValidator {
	return &rsaSHASignatureValidator{
		key: key,
	}
}

func (sv *rsaSHASignatureValidator) ValidateSignature(algorithm string, keyID *string, headerAndPayload string, signature []byte) bool {
	var hashType crypto.Hash
	var hasher hash.Hash
	var pssOpts *rsa.PSSOptions
	switch algorithm {
	case "RS256":
		hashType = crypto.SHA256
		hasher = sha256.New()
	case "RS384":
		hashType = crypto.SHA384
		hasher = sha512.New384()
	case "RS512":
		hashType = crypto.SHA512
		hasher = sha512.New()
	case "PS256":
		hashType = crypto.SHA256
		hasher = sha256.New()
		pssOpts = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA256}
	case "PS384":
		hashType = crypto.SHA384
		hasher = sha512.New384()
		pssOpts = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA384}
	case "PS512":
		hashType = crypto.SHA512
		hasher = sha512.New()
		pssOpts = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA512}
	default:
		return false
	}
	hasher.Write([]byte(headerAndPayload))
	digest := hasher.Sum(nil)
	if pssOpts != nil {
		return rsa.VerifyPSS(sv.key, hashType, digest, signature, pssOpts) == nil
	}
	return rsa.VerifyPKCS1v15(sv.key, hashType, digest, signature) == nil
}
