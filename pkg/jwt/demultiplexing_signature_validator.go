package jwt

type demultiplexingSignatureValidator struct {
	namedSignatureValidators map[string]SignatureValidator
	allSignatureValidators   []SignatureValidator
}

// NewDemultiplexingSignatureValidator creates a SignatureValidator that
// routes signature validation requests based on the key ID ("kid")
// field that's part of a JWT's header.
func NewDemultiplexingSignatureValidator(namedSignatureValidators map[string]SignatureValidator, allSignatureValidators []SignatureValidator) SignatureValidator {
	return &demultiplexingSignatureValidator{
		namedSignatureValidators: namedSignatureValidators,
		allSignatureValidators:   allSignatureValidators,
	}
}

func (sv *demultiplexingSignatureValidator) ValidateSignature(algorithm string, keyID *string, headerAndPayload string, signature []byte) bool {
	if keyID == nil {
		// No key ID provided. Simply try all signature validators.
		for _, signatureValidator := range sv.allSignatureValidators {
			if signatureValidator.ValidateSignature(algorithm, keyID, headerAndPayload, signature) {
				return true
			}
		}
	} else if signatureValidator, ok := sv.namedSignatureValidators[*keyID]; ok {
		// Exact match on the key ID.
		return signatureValidator.ValidateSignature(algorithm, keyID, headerAndPayload, signature)
	}
	return false
}
