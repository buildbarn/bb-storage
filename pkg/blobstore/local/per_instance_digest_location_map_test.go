package local_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPerInstanceDigestLocationMap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validDLM := mock.NewMockDigestLocationMap(ctrl)
	dlm := local.NewPerInstanceDigestLocationMap(
		map[string]local.DigestLocationMap{
			"valid": validDLM,
		})

	validDigest := util.MustNewDigest(
		"valid",
		&remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 123,
		})
	invalidDigest := util.MustNewDigest(
		"invalid",
		&remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 123,
		})
	validator := local.LocationValidator{
		OldestBlockID: 12,
		NewestBlockID: 15,
	}
	location := local.Location{
		BlockID:   14,
		Offset:    21,
		SizeBytes: 30,
	}

	t.Run("GetValid", func(t *testing.T) {
		validDLM.EXPECT().Get(validDigest, &validator).Return(location, nil)
		l, err := dlm.Get(validDigest, &validator)
		require.NoError(t, err)
		require.Equal(t, location, l)
	})

	t.Run("GetInvalid", func(t *testing.T) {
		_, err := dlm.Get(invalidDigest, &validator)
		require.Equal(t, status.Error(codes.InvalidArgument, "Invalid instance name: \"invalid\""), err)
	})

	t.Run("PutValid", func(t *testing.T) {
		validDLM.EXPECT().Put(validDigest, &validator, location).Return(nil)
		err := dlm.Put(validDigest, &validator, location)
		require.NoError(t, err)
	})

	t.Run("PutInvalid", func(t *testing.T) {
		err := dlm.Put(invalidDigest, &validator, location)
		require.Equal(t, status.Error(codes.InvalidArgument, "Invalid instance name: \"invalid\""), err)
	})
}
