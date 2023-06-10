package jwt

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"reflect"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/jwt"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewAuthorizationHeaderParserFromConfiguration creates a new HTTP
// "Authorization" header parser based on options stored in a
// configuration file.
func NewAuthorizationHeaderParserFromConfiguration(config *configuration.AuthorizationHeaderParserConfiguration) (*AuthorizationHeaderParser, error) {
	var signatureValidator SignatureValidator
	switch key := config.Key.(type) {
	case *configuration.AuthorizationHeaderParserConfiguration_HmacKey:
		signatureValidator = NewHMACSHASignatureValidator(key.HmacKey)
	case *configuration.AuthorizationHeaderParserConfiguration_PublicKey:
		block, _ := pem.Decode([]byte(key.PublicKey))
		if block == nil {
			return nil, status.Error(codes.InvalidArgument, "Public key does not use the PEM format")
		}
		parsedKey, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse public key")
		}
		switch convertedKey := parsedKey.(type) {
		case *ecdsa.PublicKey:
			var err error
			signatureValidator, err = NewECDSASHASignatureValidator(convertedKey)
			if err != nil {
				return nil, err
			}
		case ed25519.PublicKey:
			signatureValidator = NewEd25519SignatureValidator(convertedKey)
		case *rsa.PublicKey:
			signatureValidator = NewRSASHASignatureValidator(convertedKey)
		default:
			keyType := reflect.TypeOf(parsedKey)
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported public key type: %s/%s", keyType.PkgPath(), keyType.Name())
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "No key type provided")
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
