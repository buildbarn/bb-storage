package jwt

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/json"
	"reflect"

	jose "github.com/go-jose/go-jose/v3"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type jwksSignatureValidator struct {
	validators map[string]SignatureValidator
}

// FIXME
// NewJWKSignatureValidator creates a SignatureValidator...
func NewJWKSSignatureValidator(jwks []byte) (SignatureValidator, error) {
	validators := make(map[string]SignatureValidator)

	var keySet jose.JSONWebKeySet
	err := json.Unmarshal(jwks, &keySet)
	if err != nil {
		return nil, err
	}

	for _, k := range keySet.Keys {
		if !k.Valid() {
			// Should this be fatal?
			continue
		}

		switch key := k.Key.(type) {
		case *ecdsa.PublicKey:
			val, err := NewECDSASHASignatureValidator(key)
			if err != nil {
				return nil, err
			}
			validators[k.KeyID] = val
		case ed25519.PublicKey:
			validators[k.KeyID] = NewEd25519SignatureValidator(key)
		case *rsa.PublicKey:
			validators[k.KeyID] = NewRSASHASignatureValidator(key)
		case []byte:
			validators[k.KeyID] = NewHMACSHASignatureValidator(key)
		default:
			keyType := reflect.TypeOf(k.Key)
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported public key type: %s/%s", keyType.PkgPath(), keyType.Name())
		}
	}

	return &jwksSignatureValidator{
		validators: validators,
	}, nil
}

func (sv *jwksSignatureValidator) ValidateSignature(algorithm, keyId, headerAndPayload string, signature []byte) bool {
	val, ok := sv.validators[keyId]
	if !ok {
		return false
	}

	return val.ValidateSignature(algorithm, keyId, headerAndPayload, signature)
}
