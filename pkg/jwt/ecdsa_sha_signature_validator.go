package jwt

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"math/big"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ecdsaSHAParameters struct {
	algorithm    string
	hashFunc     func() hash.Hash
	keySizeBytes int
}

var supportedECDSASHAParameters = map[int]*ecdsaSHAParameters{
	256: {
		algorithm:    "ES256",
		hashFunc:     sha256.New,
		keySizeBytes: 32,
	},
	384: {
		algorithm:    "ES384",
		hashFunc:     sha512.New384,
		keySizeBytes: 48,
	},
	521: {
		algorithm:    "ES512",
		hashFunc:     sha512.New,
		keySizeBytes: 66,
	},
}

type ecdsaSHASignatureValidator struct {
	publicKey  *ecdsa.PublicKey
	parameters *ecdsaSHAParameters
}

// NewECDSASHASignatureValidator creates a SignatureValidator that
// expects the signature of a JWT to use the Elliptic Curve Digital
// Signature Algorithm (ECDSA), using SHA-256, SHA-384 or SHA-512 as a
// hashing algorithm.
//
// ECDSA uses asymmetrical cryptography, meaning that signing is
// performed using a private key, while verification only relies on a
// public key.
func NewECDSASHASignatureValidator(publicKey *ecdsa.PublicKey) (SignatureValidator, error) {
	bitSize := publicKey.Curve.Params().BitSize
	parameters, ok := supportedECDSASHAParameters[bitSize]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Public key has an invalid bit size: %d", bitSize)
	}
	return &ecdsaSHASignatureValidator{
		publicKey:  publicKey,
		parameters: parameters,
	}, nil
}

func (sv *ecdsaSHASignatureValidator) ValidateSignature(algorithm, keyId, headerAndPayload string, signature []byte) bool {
	p := sv.parameters
	if algorithm != p.algorithm || len(signature) != 2*p.keySizeBytes {
		return false
	}
	hash := p.hashFunc()
	hash.Write([]byte(headerAndPayload))
	r := big.NewInt(0).SetBytes(signature[:p.keySizeBytes])
	s := big.NewInt(0).SetBytes(signature[p.keySizeBytes:])
	return ecdsa.Verify(sv.publicKey, hash.Sum(nil), r, s)
}
