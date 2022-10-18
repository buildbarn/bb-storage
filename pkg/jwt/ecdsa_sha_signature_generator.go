package jwt

import (
	"crypto/ecdsa"

	"github.com/buildbarn/bb-storage/pkg/random"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ecdsaSHASignatureGenerator struct {
	privateKey            *ecdsa.PrivateKey
	parameters            *ecdsaSHAParameters
	randomNumberGenerator random.ThreadSafeGenerator
}

// NewECDSASHASignatureGenerator creates a SignatureGenerator that can
// sign a JWT using the Elliptic Curve Digital Signature Algorithm
// (ECDSA), using SHA-256, SHA-384 or SHA-512 as a hashing algorithm.
//
// ECDSA uses asymmetrical cryptography, meaning that signing is
// performed using a private key, while verification only relies on a
// public key.
func NewECDSASHASignatureGenerator(privateKey *ecdsa.PrivateKey, randomNumberGenerator random.ThreadSafeGenerator) (SignatureGenerator, error) {
	bitSize := privateKey.PublicKey.Curve.Params().BitSize
	parameters, ok := supportedECDSASHAParameters[bitSize]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Private key has an invalid bit size: %d", bitSize)
	}
	return &ecdsaSHASignatureGenerator{
		privateKey:            privateKey,
		parameters:            parameters,
		randomNumberGenerator: randomNumberGenerator,
	}, nil
}

func (sc *ecdsaSHASignatureGenerator) GetAlgorithm() string {
	return sc.parameters.algorithm
}

func (sc *ecdsaSHASignatureGenerator) GenerateSignature(headerAndPayload string) ([]byte, error) {
	p := sc.parameters
	hash := p.hashFunc()
	hash.Write([]byte(headerAndPayload))
	r, s, err := ecdsa.Sign(sc.randomNumberGenerator, sc.privateKey, hash.Sum(nil))
	if err != nil {
		return nil, err
	}
	signature := make([]byte, 2*p.keySizeBytes)
	r.FillBytes(signature[:p.keySizeBytes])
	s.FillBytes(signature[p.keySizeBytes:])
	return signature, nil
}
