package grpcservers_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestInitialSizeClassCacheServer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	statsReader := mock.NewMockMessageReader[*iscc.PreviousExecutionStats](ctrl)

	server := grpcservers.NewInitialSizeClassCacheServer(blobAccess, statsReader)

	t.Run("GetPreviousExecutionStatsSuccess", func(t *testing.T) {
		digest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "09f7e02f1290be211da707a266f153b3", 100)
		stats := &iscc.PreviousExecutionStats{}

		statsReader.EXPECT().ReadMessage(
			gomock.Any(),
			digest,
			gomock.Any(),
		).Return(stats, nil)

		resp, err := server.GetPreviousExecutionStats(ctx, &iscc.GetPreviousExecutionStatsRequest{
			InstanceName:   "ubuntu1804",
			DigestFunction: remoteexecution.DigestFunction_MD5,
			ReducedActionDigest: &remoteexecution.Digest{
				Hash:      "09f7e02f1290be211da707a266f153b3",
				SizeBytes: 100,
			},
		})

		require.NoError(t, err)
		require.Equal(t, stats, resp)
	})
}
