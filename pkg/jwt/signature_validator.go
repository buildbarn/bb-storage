package jwt

// SignatureValidator is used by Authenticator to validate the signature
// of a JWT. Implementations of this interface may use HMAC, ECDSA or
// other algorithms.
type SignatureValidator interface {
	ValidateSignature(algorithm string, keyID *string, headerAndPayload string, signature []byte) bool
}
