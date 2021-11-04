package jwt

// SignatureGenerator is used by GenerateAuthorizationHeader() to create
// the signature of a JWT. Implementations of this interface may use
// HMAC, ECDSA or other algorithms.
type SignatureGenerator interface {
	GetAlgorithm() string
	GenerateSignature(headerAndPayload string) ([]byte, error)
}
