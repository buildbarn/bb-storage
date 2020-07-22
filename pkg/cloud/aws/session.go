package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/cloud/aws"
)

// NewSessionFromConfiguration creates a new AWS SDK session object
// based on options specified in a session configuration message. The
// resulting session object can be used to access AWS services such as
// EC2, S3 and SQS.
func NewSessionFromConfiguration(configuration *aws_pb.SessionConfiguration) (*session.Session, error) {
	var cfg aws.Config
	if endpoint := configuration.GetEndpoint(); endpoint != "" {
		cfg.Endpoint = aws.String(endpoint)
	}
	if region := configuration.GetRegion(); region != "" {
		cfg.Region = aws.String(region)
	}
	if configuration.GetDisableSsl() {
		cfg.DisableSSL = aws.Bool(true)
	}
	if configuration.GetS3ForcePathStyle() {
		cfg.S3ForcePathStyle = aws.Bool(true)
	}
	if staticCredentials := configuration.GetStaticCredentials(); staticCredentials != nil {
		cfg.Credentials = credentials.NewStaticCredentials(
			staticCredentials.AccessKeyId,
			staticCredentials.SecretAccessKey,
			"")
	}
	return session.NewSession(&cfg)
}
