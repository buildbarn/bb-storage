package jwt

import (
	"sync/atomic"
)

type forwardingSignatureValidator struct {
	validator atomic.Pointer[SignatureValidator]
}

// NewForwardingSignatureValidator creates a SignatureValidator that simply forwards
// requests to another SignatureValidator.
func NewForwardingSignatureValidator(validator SignatureValidator) SignatureValidator {
	sv := &forwardingSignatureValidator{}
	sv.validator.Store(&validator)

	return sv
}

func (sv *forwardingSignatureValidator) Replace(validator SignatureValidator) {
	sv.validator.Store(&validator)
}

func (sv *forwardingSignatureValidator) ValidateSignature(algorithm string, keyID *string, headerAndPayload string, signature []byte) bool {
	return (*sv.validator.Load()).ValidateSignature(algorithm, keyID, headerAndPayload, signature)
}
