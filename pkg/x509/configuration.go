package x509

import (
	"crypto/x509"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/jmespath"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/x509"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewClientCertificateVerifierFromConfiguration creates a new X.509
// client certificate verifier based on options provided in a
// configuration file.
func NewClientCertificateVerifierFromConfiguration(configuration *pb.ClientCertificateVerifierConfiguration, group program.Group) (*ClientCertificateVerifier, error) {
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM([]byte(configuration.ClientCertificateAuthorities)) {
		return nil, status.Error(codes.InvalidArgument, "Failed to parse client certificate authorities")
	}
	validator, err := jmespath.NewExpressionFromConfiguration(configuration.ValidationJmespathExpression, group, clock.SystemClock)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to compile validation JMESPath expression")
	}
	metadataExtractor, err := jmespath.NewExpressionFromConfiguration(configuration.MetadataExtractionJmespathExpression, group, clock.SystemClock)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to compile metadata extraction JMESPath expression")
	}
	return NewClientCertificateVerifier(
		clientCAs,
		clock.SystemClock,
		validator,
		metadataExtractor,
	), nil
}
