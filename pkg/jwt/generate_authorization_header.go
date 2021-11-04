package jwt

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

// GenerateAuthorizationHeader can be used to create HTTP
// "Authorization" headers of shape "Bearer ${jwt}". It calls into a
// SignatureGenerator to sign the resulting header.
func GenerateAuthorizationHeader(payload interface{}, signatureGenerator SignatureGenerator) (string, error) {
	// Generate header and payload.
	header := struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}{
		Alg: signatureGenerator.GetAlgorithm(),
		Typ: "JWT",
	}
	headerJSON, err := json.Marshal(&header)
	if err != nil {
		return "", util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal header")
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal payload")
	}
	headerAndPayload := fmt.Sprintf(
		"%s.%s",
		base64.RawURLEncoding.EncodeToString(headerJSON),
		base64.RawURLEncoding.EncodeToString(payloadJSON))

	// Sign header and payload.
	signature, err := signatureGenerator.GenerateSignature(headerAndPayload)
	if err != nil {
		return "", util.StatusWrap(err, "Failed to generate signature")
	}
	return fmt.Sprintf(
		"Bearer %s.%s",
		headerAndPayload,
		base64.RawURLEncoding.EncodeToString(signature)), nil
}
