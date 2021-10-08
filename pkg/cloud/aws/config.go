package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aws_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/cloud/aws"
)

// NewConfigFromConfiguration creates a new AWS SDK config object based
// on options specified in a session configuration message. The
// resulting session object can be used to access AWS services such as
// EC2, S3 and SQS.
func NewConfigFromConfiguration(configuration *aws_pb.SessionConfiguration) (aws.Config, error) {
	loadOptions := []func(*config.LoadOptions) error{}
	if region := configuration.GetRegion(); region != "" {
		loadOptions = append(loadOptions, config.WithRegion(region))
	}
	if staticCredentials := configuration.GetStaticCredentials(); staticCredentials != nil {
		loadOptions = append(loadOptions,
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					staticCredentials.AccessKeyId,
					staticCredentials.SecretAccessKey,
					"")))
	}
	return config.LoadDefaultConfig(context.Background(), loadOptions...)
}
