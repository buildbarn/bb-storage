package cloud_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cloud"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestS3CopyMutator(t *testing.T) {
	ctrl := gomock.NewController(t)

	minRefreshAge := 17 * time.Second
	clock := mock.NewMockClock(ctrl)
	now := time.Unix(1000, 0)
	clock.EXPECT().Now().Return(now)

	mutator := cloud.NewS3LRURefreshingBeforeCopyFunc(minRefreshAge, clock)
	var input s3.CopyObjectInput
	require.NoError(t, mutator(func(i interface{}) bool {
		// https://github.com/google/go-cloud/blob/master/blob/s3blob/s3blob.go
		// does it this way, so we follow.
		switch v := i.(type) {
		case **s3.CopyObjectInput:
			*v = &input
			return true
		}
		return false
	}))

	require.Equal(t, s3.CopyObjectInput{
		Metadata:                    aws.StringMap(map[string]string{"Used": now.String()}),
		MetadataDirective:           aws.String("REPLACE"),
		CopySourceIfUnmodifiedSince: aws.Time(time.Unix(1000-17, 0)),
	}, input)
}
