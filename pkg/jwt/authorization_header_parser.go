package jwt

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"regexp"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/jmespath/go-jmespath"
)

// Pattern of authorization headers from which to extract a JSON Web Token.
var jwtHeaderPattern = regexp.MustCompile("^Bearer\\s+(([-_a-zA-Z0-9]+)\\.([-_a-zA-Z0-9]+))\\.([-_a-zA-Z0-9]+)$")

type response struct {
	notBefore      time.Time
	expirationTime time.Time
}

func (r *response) isAuthenticated(now time.Time) bool {
	return !now.Before(r.notBefore) && now.Before(r.expirationTime)
}

var (
	farHistory = time.Unix(0, 0)
	farFuture  = time.Unix(math.MaxInt64/2, 0)

	// Authorization response we can use in case a token is invalid.
	unauthenticated = response{
		notBefore:      farFuture,
		expirationTime: farHistory,
	}
)

// AuthorizationHeaderParser is a helper type for parsing JSON Web
// Tokens stored in HTTP "Authorization" headers of shape "Bearer ${jwt}".
// To prevent wasting excessive amounts of processing time on signature
// verification, this type holds on to a cache of recently observed
// tokens.
type AuthorizationHeaderParser struct {
	clock              clock.Clock
	signatureValidator SignatureValidator
	claimsValidator    *jmespath.JMESPath
	maximumCacheSize   int

	lock                       sync.Mutex
	cachedAuthorizationHeaders map[string]response
	evictionSet                eviction.Set
}

// NewAuthorizationHeaderParser creates a new AuthorizationHeaderParser
// that does not have any cached tokens.
func NewAuthorizationHeaderParser(clock clock.Clock, signatureValidator SignatureValidator, claimsValidator *jmespath.JMESPath, maximumCacheSize int, evictionSet eviction.Set) *AuthorizationHeaderParser {
	return &AuthorizationHeaderParser{
		clock:              clock,
		signatureValidator: signatureValidator,
		claimsValidator:    claimsValidator,
		maximumCacheSize:   maximumCacheSize,

		cachedAuthorizationHeaders: map[string]response{},
		evictionSet:                evictionSet,
	}
}

func jsonNumberAsTimestamp(n *json.Number) (time.Time, error) {
	if v, err := n.Int64(); err == nil {
		return time.Unix(v, 0), nil
	}
	v, err := n.Float64()
	if err != nil {
		return time.Time{}, err
	}
	i, frac := math.Modf(v)
	return time.Unix(int64(i), int64(frac*1e9)), nil
}

func (a *AuthorizationHeaderParser) parseSingleAuthorizationHeader(header string, now time.Time) response {
	match := jwtHeaderPattern.FindStringSubmatch(header)
	if match == nil {
		return unauthenticated
	}

	// Decode base64 for all three components of the token.
	decodedFields := make([][]byte, 0, 3)
	for _, field := range match[2:] {
		decodedField, err := base64.RawURLEncoding.DecodeString(field)
		if err != nil {
			return unauthenticated
		}
		decodedFields = append(decodedFields, decodedField)
	}

	// Perform signature validation.
	headerMessage := struct {
		Alg string `json:"alg"`
	}{}
	if json.Unmarshal(decodedFields[0], &headerMessage) != nil {
		return unauthenticated
	}
	if !a.signatureValidator.ValidateSignature(headerMessage.Alg, match[1], decodedFields[2]) {
		return unauthenticated
	}

	// Perform validation of additional claims using JMESPath.
	var fullPayloadMessage interface{}
	if json.Unmarshal(decodedFields[1], &fullPayloadMessage) != nil {
		return unauthenticated
	}
	if result, err := a.claimsValidator.Search(fullPayloadMessage); err != nil || result != true {
		return unauthenticated
	}

	// Extract timestamps.
	payloadMessage := struct {
		Exp *json.Number `json:"exp"`
		Nbf *json.Number `json:"nbf"`
	}{}
	if json.Unmarshal(decodedFields[1], &payloadMessage) != nil {
		return unauthenticated
	}
	r := response{
		notBefore:      farHistory,
		expirationTime: farFuture,
	}
	if nbf := payloadMessage.Nbf; nbf != nil {
		// Extract "nbf" (Not Before) claim.
		v, err := jsonNumberAsTimestamp(nbf)
		if err != nil {
			return unauthenticated
		}
		r.notBefore = v
	}
	if exp := payloadMessage.Exp; exp != nil {
		// Extract "exp" (Expiration Time) claim.
		v, err := jsonNumberAsTimestamp(exp)
		if err != nil {
			return unauthenticated
		}
		r.expirationTime = v
	}
	return r
}

// ParseAuthorizationHeaders takes a set of HTTP "Authorization" headers
// and returned true if one or more headers contain a token whose
// signature can be validated, and whose "exp" (Expiration Time) and
// "nbf" (Not Before) claims are in bounds.
func (a *AuthorizationHeaderParser) ParseAuthorizationHeaders(headers []string) bool {
	now := a.clock.Now()

	a.lock.Lock()
	defer a.lock.Unlock()

	// Check whether any of the authorization headers have been
	// presented before. If so, skip token validation entirely.
	headersToCheck := make([]string, 0, len(headers))
	for _, header := range headers {
		if response, ok := a.cachedAuthorizationHeaders[header]; ok {
			a.evictionSet.Touch(header)
			if response.isAuthenticated(now) {
				return true
			}
		} else {
			headersToCheck = append(headersToCheck, header)
		}
	}

	// Token is not cached. Validate it and cache its expiration time.
	for _, header := range headersToCheck {
		response := a.parseSingleAuthorizationHeader(header, now)
		for len(a.cachedAuthorizationHeaders) >= a.maximumCacheSize {
			delete(a.cachedAuthorizationHeaders, a.evictionSet.Peek())
			a.evictionSet.Remove()
		}
		if _, ok := a.cachedAuthorizationHeaders[header]; ok {
			a.evictionSet.Touch(header)
		} else {
			a.evictionSet.Insert(header)
		}
		a.cachedAuthorizationHeaders[header] = response
		if response.isAuthenticated(now) {
			return true
		}
	}
	return false
}
