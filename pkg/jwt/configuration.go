package jwt

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/json"
	"reflect"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/jwt"
	"github.com/buildbarn/bb-storage/pkg/util"
	jose "github.com/go-jose/go-jose/v3"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// NewAuthorizationHeaderParserFromConfiguration creates a new HTTP
// "Authorization" header parser based on options stored in a
// configuration file.
func NewAuthorizationHeaderParserFromConfiguration(config *configuration.AuthorizationHeaderParserConfiguration) (*AuthorizationHeaderParser, error) {
	jwksJSON, err := protojson.Marshal(config.JwksInline)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal JSON Web Key Set")
	}
	var jwks jose.JSONWebKeySet
	if err := json.Unmarshal(jwksJSON, &jwks); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to unmarshal JSON Web Key Set")
	}
	signatureValidator, err := NewSignatureValidatorFromJSONWebKeySet(&jwks)
	if err != nil {
		return nil, err
	}

	evictionSet, err := eviction.NewSetFromConfiguration[string](config.CacheReplacementPolicy)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create eviction set")
	}

	claimsValidator, err := jmespath.Compile(config.ClaimsValidationJmespathExpression)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to compile claims validation JMESPath expression")
	}
	metadataExtractor, err := jmespath.Compile(config.MetadataExtractionJmespathExpression)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to compile metadata extraction JMESPath expression")
	}

	return NewAuthorizationHeaderParser(
		clock.SystemClock,
		signatureValidator,
		claimsValidator,
		metadataExtractor,
		int(config.MaximumCacheSize),
		eviction.NewMetricsSet(evictionSet, "AuthorizationHeaderParser")), nil
}

// NewSignatureValidatorFromJSONWebKeySet creates a new
// SignatureValidator capable of validating JWTs matching keys contained
// in a JSON Web Key Set, as described in RFC 7517, chapter 5.
func NewSignatureValidatorFromJSONWebKeySet(jwks *jose.JSONWebKeySet) (SignatureValidator, error) {
	namedSignatureValidators := make(map[string]SignatureValidator, len(jwks.Keys))
	allSignatureValidators := make([]SignatureValidator, 0, len(jwks.Keys))
	for i, jwk := range jwks.Keys {
		if !jwk.Valid() {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid JSON Web Key at index %d", i)
		}

		var signatureValidator SignatureValidator
		switch convertedKey := jwk.Key.(type) {
		case *ecdsa.PublicKey:
			var err error
			signatureValidator, err = NewECDSASHASignatureValidator(convertedKey)
			if err != nil {
				return nil, util.StatusWrapf(err, "Invalid ECDSA key at index %d", i)
			}
		case ed25519.PublicKey:
			signatureValidator = NewEd25519SignatureValidator(convertedKey)
		case *rsa.PublicKey:
			signatureValidator = NewRSASHASignatureValidator(convertedKey)
		default:
			keyType := reflect.TypeOf(jwk.Key)
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported public key type at index %d: %s/%s", i, keyType.PkgPath(), keyType.Name())
		}

		if jwk.KeyID != "" {
			// JSON Web Key contains a key ID. Ensure that
			// JWTs that contain an explicit key ID only get
			// matched to this validator if the key ID
			// matches.
			if _, ok := namedSignatureValidators[jwk.KeyID]; ok {
				return nil, status.Errorf(codes.InvalidArgument, "JSON Web Key Set contains multiple keys with ID %#v", jwk.KeyID)
			}
			namedSignatureValidators[jwk.KeyID] = signatureValidator
		}
		allSignatureValidators = append(allSignatureValidators, signatureValidator)
	}

	return NewDemultiplexingSignatureValidator(namedSignatureValidators, allSignatureValidators), nil
}
