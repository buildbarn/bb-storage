package cloud_test

import (
	"testing"
	"time"

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

	mutator := cloud.NewS3CopyMutator(minRefreshAge, clock)
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

	require.Equal(t, now.String(), *input.Metadata["Used"])
	require.Equal(t, "REPLACE", *input.MetadataDirective)
	require.Equal(t, time.Unix(1000-17, 0), *input.CopySourceIfUnmodifiedSince)
}
