package jwt

import (
	"sync/atomic"
)

// ForwardingSignatureValidator wraps another SignatureValidator. It is
// used when the underlying SignatureValidator needs to be replaced at
// runtime.
type ForwardingSignatureValidator struct {
	validator atomic.Pointer[SignatureValidator]
}

// NewForwardingSignatureValidator creates a SignatureValidator that simply forwards
// requests to another SignatureValidator.
// This returns a pointer to the new ForwardingSignatureValidator, so as not to
// copy the atomic.Pointer.
func NewForwardingSignatureValidator(validator SignatureValidator) *ForwardingSignatureValidator {
	sv := ForwardingSignatureValidator{}
	sv.validator.Store(&validator)

	return &sv
}

// Replace the registered SignatureValidator
func (sv *ForwardingSignatureValidator) Replace(validator SignatureValidator) {
	sv.validator.Store(&validator)
}

// Validate a signature using the registered SignatureValidator
func (sv *ForwardingSignatureValidator) ValidateSignature(algorithm string, keyID *string, headerAndPayload string, signature []byte) bool {
	return (*sv.validator.Load()).ValidateSignature(algorithm, keyID, headerAndPayload, signature)
}
