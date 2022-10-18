package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client is an interface around the AWS SDK S3 client. It has been
// added to aid unit testing.
type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

var _ S3Client = &s3.Client{}
