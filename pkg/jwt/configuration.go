package jwt

import (
	"os"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/jwt"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// NewAuthorizationHeaderParserFromConfiguration creates a new HTTP
// "Authorization" header parser based on options stored in a
// configuration file.
func NewAuthorizationHeaderParserFromConfiguration(config *configuration.AuthorizationHeaderParserConfiguration) (*AuthorizationHeaderParser, error) {
	var err error
	var jwksJson []byte

	switch key := config.Jwks.(type) {
	case *configuration.AuthorizationHeaderParserConfiguration_JwksInline:
		jwksJson, err = protojson.Marshal(key.JwksInline)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse inline JWKS")
		}
	case *configuration.AuthorizationHeaderParserConfiguration_JwksPath:
		jwksJson, err = os.ReadFile(key.JwksPath)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to read JWKS file")
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "No key type provided")
	}

	signatureValidator, err := NewJWKSSignatureValidator(jwksJson)

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
