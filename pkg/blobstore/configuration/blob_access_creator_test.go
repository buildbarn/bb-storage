package configuration_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBlobAccessCreatorGetDigestKeyFormat(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBlobAccess := mock.NewMockBlobAccess(ctrl)
	grpcClientFactory := mock.NewMockClientFactory(ctrl)
	helloDigest := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)

	// Require that both the StorageType and the
	// BlobReplicatorCreator provide consistent mechanisms for
	// creating keys from digests. Failure to do so could cause
	// MirroredBlobAccess to not replicate objects, or replicate
	// them unnecessarily.
	for _, bac := range []configuration.BlobAccessCreator{
		configuration.NewACBlobAccessCreator(mockBlobAccess, grpcClientFactory, 123),
		configuration.NewCASBlobAccessCreator(grpcClientFactory, 123),
		configuration.NewICASBlobAccessCreator(grpcClientFactory, 123),
	} {
		require.Equal(t,
			helloDigest.GetKey(bac.GetDigestKeyFormat()),
			bac.GetStorageType().GetDigestKey(helloDigest))
	}
}
