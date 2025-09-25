package aws

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	http_client "github.com/buildbarn/bb-storage/pkg/http/client"
	aws_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewConfigFromConfiguration creates a new AWS SDK config object based
// on options specified in a session configuration message. The
// resulting session object can be used to access AWS services such as
// EC2, S3 and SQS.
func NewConfigFromConfiguration(configuration *aws_pb.SessionConfiguration, name string) (aws.Config, error) {
	roundTripper, err := http_client.NewRoundTripperFromConfiguration(configuration.GetHttpClient())
	if err != nil {
		return aws.Config{}, util.StatusWrap(err, "Failed to create HTTP client")
	}
	loadOptions := []func(*config.LoadOptions) error{
		config.WithHTTPClient(&http.Client{
			Transport: http_client.NewMetricsRoundTripper(roundTripper, name),
		}),
	}
	if region := configuration.GetRegion(); region != "" {
		loadOptions = append(loadOptions, config.WithRegion(region))
	}
	if credentialsOptions := configuration.GetCredentials(); credentialsOptions != nil {
		switch credentialsType := credentialsOptions.(type) {
		case *aws_pb.SessionConfiguration_StaticCredentials:
			// Use static credentials.
			staticCredentials := credentialsType.StaticCredentials
			loadOptions = append(loadOptions,
				config.WithCredentialsProvider(
					credentials.NewStaticCredentialsProvider(
						staticCredentials.AccessKeyId,
						staticCredentials.SecretAccessKey,
						"")))
		case *aws_pb.SessionConfiguration_WebIdentityRoleCredentials:
			// Use web identity role credentials. This
			// provider depends on an STS client, so we
			// first create an AWS config that does not have
			// any credentials set.
			webIdentityRoleCredentials := credentialsType.WebIdentityRoleCredentials
			configWithoutCredentials, err := config.LoadDefaultConfig(context.Background(), loadOptions...)
			if err != nil {
				return aws.Config{}, err
			}
			loadOptions = append(loadOptions,
				config.WithCredentialsProvider(
					stscreds.NewWebIdentityRoleProvider(
						sts.NewFromConfig(configWithoutCredentials),
						webIdentityRoleCredentials.RoleArn,
						stscreds.IdentityTokenFile(webIdentityRoleCredentials.TokenFile))))
		default:
			return aws.Config{}, status.Error(codes.InvalidArgument, "Unknown credentials options type provided")
		}
	}
	return config.LoadDefaultConfig(context.Background(), loadOptions...)
}
