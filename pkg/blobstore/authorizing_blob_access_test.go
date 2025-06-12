package blobstore_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestAuthorizingBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	getAuthorizer := mock.NewMockAuthorizer(ctrl)
	putAuthorizer := mock.NewMockAuthorizer(ctrl)
	findMissingAuthorizer := mock.NewMockAuthorizer(ctrl)
	ba := blobstore.NewAuthorizingBlobAccess(baseBlobAccess, getAuthorizer, putAuthorizer, findMissingAuthorizer)
	d := digest.MustNewDigest("beep", remoteexecution.DigestFunction_SHA256, "693d8db7b05e99c6b7a7c0616456039d89c555029026936248085193559a0b5d", 16)
	d2 := digest.MustNewDigest("bop/bip", remoteexecution.DigestFunction_SHA256, "da95ccd92a874d2169839cd90d9045be61d17df779fb28fe520a7465c6063723", 3)
	digests := digest.GetUnion([]digest.Set{d.ToSingletonSet(), d2.ToSingletonSet()})
	wantBytes := []byte("European Burmese")
	wantBuf := buffer.NewValidatedBufferFromByteSlice(wantBytes)

	beep := util.Must(digest.NewInstanceName("beep"))
	beepSlice := []digest.InstanceName{beep}
	bopBip := util.Must(digest.NewInstanceName("bop/bip"))
	beepBopBipSlice := []digest.InstanceName{beep, bopBip}

	t.Run("Get-Allowed", func(t *testing.T) {
		getAuthorizer.EXPECT().Authorize(ctx, beepSlice).Return([]error{nil})
		baseBlobAccess.EXPECT().Get(ctx, d).Return(wantBuf)

		gotBuf, err := ba.Get(ctx, d).ToByteSlice(30)
		require.NoError(t, err)
		require.Equal(t, wantBytes, gotBuf)
	})

	t.Run("Get-Denied", func(t *testing.T) {
		getAuthorizer.EXPECT().Authorize(ctx, beepSlice).Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})

		_, err := ba.Get(ctx, d).ToByteSlice(30)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), err)
	})

	t.Run("GetFromComposite-Allowed", func(t *testing.T) {
		getAuthorizer.EXPECT().Authorize(ctx, beepSlice).Return([]error{nil})
		blobSlicer := mock.NewMockBlobSlicer(ctrl)
		baseBlobAccess.EXPECT().GetFromComposite(ctx, d, d2, blobSlicer).Return(wantBuf)

		gotBuf, err := ba.GetFromComposite(ctx, d, d2, blobSlicer).ToByteSlice(30)
		require.NoError(t, err)
		require.Equal(t, wantBytes, gotBuf)
	})

	t.Run("GetFromComposite-Denied", func(t *testing.T) {
		blobSlicer := mock.NewMockBlobSlicer(ctrl)
		getAuthorizer.EXPECT().Authorize(ctx, beepSlice).Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})

		_, err := ba.GetFromComposite(ctx, d, d2, blobSlicer).ToByteSlice(30)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), err)
	})

	t.Run("Put-Allowed", func(t *testing.T) {
		putAuthorizer.EXPECT().Authorize(ctx, beepSlice).Return([]error{nil})
		baseBlobAccess.EXPECT().Put(ctx, d, wantBuf).Return(nil)

		err := ba.Put(ctx, d, wantBuf)
		require.NoError(t, err)
	})

	t.Run("Put-Denied", func(t *testing.T) {
		putAuthorizer.EXPECT().Authorize(ctx, beepSlice).Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})

		err := ba.Put(ctx, d, wantBuf)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), err)
	})

	t.Run("FindMissing-Allowed", func(t *testing.T) {
		findMissingAuthorizer.EXPECT().Authorize(ctx, gomock.InAnyOrder(beepBopBipSlice)).Return([]error{nil, nil})
		baseBlobAccess.EXPECT().FindMissing(ctx, digests).Return(d2.ToSingletonSet(), nil)

		missing, err := ba.FindMissing(ctx, digests)
		require.NoError(t, err)
		require.Equal(t, d2.ToSingletonSet(), missing)
	})

	t.Run("FindMissing-PartiallyDenied", func(t *testing.T) {
		findMissingAuthorizer.EXPECT().Authorize(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, instanceNames []digest.InstanceName) []error {
			wantErr := status.Error(codes.PermissionDenied, "You shall not pass")
			require.ElementsMatch(t, instanceNames, beepBopBipSlice)
			if instanceNames[0] == beep {
				return []error{nil, wantErr}
			} else {
				return []error{wantErr, nil}
			}
		})

		_, err := ba.FindMissing(ctx, digests)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization of instance name \"bop/bip\": You shall not pass"), err)
	})
}
