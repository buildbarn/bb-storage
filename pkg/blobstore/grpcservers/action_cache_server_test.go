package grpcservers_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestActionCacheServer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	resultReader := mock.NewMockMessageReader[*remoteexecution.ActionResult](ctrl)

	server := grpcservers.NewActionCacheServer(blobAccess, resultReader)

	t.Run("GetActionResultSuccess", func(t *testing.T) {
		digest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "09f7e02f1290be211da707a266f153b3", 100)
		result := &remoteexecution.ActionResult{}

		resultReader.EXPECT().ReadMessage(
			gomock.Any(),
			digest,
			gomock.Any(),
		).Return(result, nil)

		resp, err := server.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
			InstanceName:   "ubuntu1804",
			DigestFunction: remoteexecution.DigestFunction_MD5,
			ActionDigest: &remoteexecution.Digest{
				Hash:      "09f7e02f1290be211da707a266f153b3",
				SizeBytes: 100,
			},
		})

		require.NoError(t, err)
		require.Equal(t, result, resp)
	})
}
