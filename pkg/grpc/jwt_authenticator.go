package grpc

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/clock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

// JWTKeyConfig represents an object that can return key content. It is used to bridge
// configuration to this package.
type JWTKeyConfig struct {
	Key interface{}
}

type jwtAuthenticator struct {
	verifyKey JWTKeyConfig
	clock     clock.Clock
}

// From: https://github.com/square/go-jose/blob/v2/jose-util/utils.go
func loadJSONWebKey(json []byte, pub bool) (*jose.JSONWebKey, error) {
	var jwk jose.JSONWebKey
	err := jwk.UnmarshalJSON(json)
	if err != nil {
		return nil, err
	}
	if !jwk.Valid() {
		return nil, errors.New("invalid JWK key")
	}
	if jwk.IsPublic() != pub {
		return nil, errors.New("priv/pub JWK key mismatch")
	}
	return &jwk, nil
}

// LoadJWTPublicKey loads a public key from PEM/DER/JWK-encoded data.
// From: https://github.com/square/go-jose/blob/v2/jose-util/utils.go
func loadJWTPublicKey(data []byte) (interface{}, error) {
	input := data

	block, _ := pem.Decode(data)
	if block != nil {
		input = block.Bytes
	}

	// Try to load SubjectPublicKeyInfo
	pub, err0 := x509.ParsePKIXPublicKey(input)
	if err0 == nil {
		return pub, nil
	}

	cert, err1 := x509.ParseCertificate(input)
	if err1 == nil {
		return cert.PublicKey, nil
	}

	jwk, err2 := loadJSONWebKey(data, true)
	if err2 == nil {
		return jwk, nil
	}

	return nil, fmt.Errorf("JWT setup error: parse error, got '%s', '%s' and '%s'", err0, err1, err2)
}

// NewJWTAuthenticator creates an Authenticator that
// only grants access in case a validly-signed JWT (JSON Web Token)
// is passed as a Bearer token in the request's "authorization" header.
func NewJWTAuthenticator(key JWTKeyConfig, clock clock.Clock) Authenticator {
	return &jwtAuthenticator{
		verifyKey: key,
		clock:     clock,
	}
}

func (a *jwtAuthenticator) Authenticate(ctx context.Context) error {
	// Get the gRPC metadata.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "Connection was not established using gRPC")
	}

	// Extract the `authorization` header.
	// Note: The keys within the metadata are normalized to lowercase.
	//       https://godoc.org/google.golang.org/grpc/metadata#New
	authHeaders, ok := md["authorization"]
	if !ok || len(authHeaders) < 1 {
		return status.Error(codes.Unauthenticated, "Missing authorization header")
	}

	var errs []string

	for _, authHeaderValue := range authHeaders {
		if !strings.HasPrefix(authHeaderValue, "Bearer ") {
			// Ignore non-bearer tokens (which could be provided for another Authenticator
			// implementation).
			continue
		}

		jwtString := strings.TrimPrefix(authHeaderValue, "Bearer ")

		tok, err := jwt.ParseSigned(jwtString)
		if err != nil {
			errs = append(errs, "Failed to parse signed bearer token: "+err.Error())
			continue
		}

		// Verify the signature.
		var claims jwt.Claims
		err = tok.Claims(a.verifyKey.Key, &claims)
		if err != nil {
			errs = append(errs, "Authorization required: "+err.Error())
			continue
		}

		// Signature is valid. Validate the time-related claims.
		// TODO: Validate other claims, e.g. issuer, subject, audience.
		expectedClaims := jwt.Expected{
			Time: a.clock.Now(),
		}
		err = claims.Validate(expectedClaims)
		if err == nil {
			return nil
		}
		errs = append(errs, "Authorization required: "+err.Error())
	}

	return status.Error(codes.Unauthenticated, strings.Join(errs, ", "))
}
