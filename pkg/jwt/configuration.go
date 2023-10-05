package jwt

import (
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/jwt"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/protobuf/encoding/protojson"
)

// NewAuthorizationHeaderParserFromConfiguration creates a new HTTP
// "Authorization" header parser based on options stored in a
// configuration file.
func NewAuthorizationHeaderParserFromConfiguration(config *configuration.AuthorizationHeaderParserConfiguration) (*AuthorizationHeaderParser, error) {
	var err error
	var jwksJson []byte

	jwksJson, err = protojson.Marshal(config.JwksInline)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to parse inline JWKS")
	}

	signatureValidator, err := NewJWKSSignatureValidator(jwksJson)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create signature validator")
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
