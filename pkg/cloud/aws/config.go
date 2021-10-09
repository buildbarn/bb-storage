package aws

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	aws_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// NewConfigFromConfiguration creates a new AWS SDK config object based
// on options specified in a session configuration message. The
// resulting session object can be used to access AWS services such as
// EC2, S3 and SQS.
func NewConfigFromConfiguration(configuration *aws_pb.SessionConfiguration) (aws.Config, error) {
	roundTripper, err := bb_http.NewRoundTripperFromConfiguration(configuration.GetHttpClient())
	if err != nil {
		return aws.Config{}, util.StatusWrap(err, "Failed to create HTTP client")
	}
	loadOptions := []func(*config.LoadOptions) error{
		config.WithHTTPClient(&http.Client{Transport: roundTripper}),
	}
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
