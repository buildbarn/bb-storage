package grpcservers_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestFileSystemAccessCacheServer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	profileReader := mock.NewMockMessageReader[*fsac.FileSystemAccessProfile](ctrl)

	server := grpcservers.NewFileSystemAccessCacheServer(blobAccess, profileReader)

	t.Run("GetFileSystemAccessProfileSuccess", func(t *testing.T) {
		digest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "09f7e02f1290be211da707a266f153b3", 100)
		profile := &fsac.FileSystemAccessProfile{}

		profileReader.EXPECT().ReadMessage(
			gomock.Any(),
			digest,
			gomock.Any(),
		).Return(profile, nil)

		resp, err := server.GetFileSystemAccessProfile(ctx, &fsac.GetFileSystemAccessProfileRequest{
			InstanceName:   "ubuntu1804",
			DigestFunction: remoteexecution.DigestFunction_MD5,
			ReducedActionDigest: &remoteexecution.Digest{
				Hash:      "09f7e02f1290be211da707a266f153b3",
				SizeBytes: 100,
			},
		})

		require.NoError(t, err)
		require.Equal(t, profile, resp)
	})
}
