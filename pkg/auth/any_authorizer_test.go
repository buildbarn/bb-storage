package auth_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

// Test the behavior if NewAnyAuthorizer() is called without any backends.
func TestAnyAuthorizerZero(t *testing.T) {
	authorizer := auth.NewAnyAuthorizer(nil)

	errs := authorizer.Authorize(context.Background(), []digest.InstanceName{
		util.Must(digest.NewInstanceName("hello")),
		util.Must(digest.NewInstanceName("world")),
	})
	require.Len(t, errs, 2)
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])
}

// Test the behavior if NewAnyAuthorizer() is called with one backend.
func TestAnyAuthorizerOne(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseAuthorizer := mock.NewMockAuthorizer(ctrl)
	authorizer := auth.NewAnyAuthorizer([]auth.Authorizer{baseAuthorizer})

	baseAuthorizer.EXPECT().Authorize(ctx, []digest.InstanceName{
		util.Must(digest.NewInstanceName("ok")),
		util.Must(digest.NewInstanceName("denied")),
		util.Must(digest.NewInstanceName("unavailable")),
	}).Return([]error{
		nil,
		status.Error(codes.PermissionDenied, "Permission denied"),
		status.Error(codes.Unavailable, "Server offline"),
	})

	errs := authorizer.Authorize(ctx, []digest.InstanceName{
		util.Must(digest.NewInstanceName("ok")),
		util.Must(digest.NewInstanceName("denied")),
		util.Must(digest.NewInstanceName("unavailable")),
	})
	require.Len(t, errs, 3)
	require.NoError(t, errs[0])
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), errs[2])
}

// Test the behavior if NewAnyAuthorizer() is called with three backends.
func TestAnyAuthorizerMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseAuthorizer1 := mock.NewMockAuthorizer(ctrl)
	baseAuthorizer2 := mock.NewMockAuthorizer(ctrl)
	baseAuthorizer3 := mock.NewMockAuthorizer(ctrl)
	authorizer := auth.NewAnyAuthorizer([]auth.Authorizer{
		baseAuthorizer1,
		baseAuthorizer2,
		baseAuthorizer3,
	})

	baseAuthorizer1.EXPECT().Authorize(ctx, []digest.InstanceName{
		util.Must(digest.NewInstanceName("ok1")),
		util.Must(digest.NewInstanceName("ok2")),
		util.Must(digest.NewInstanceName("ok3")),
		util.Must(digest.NewInstanceName("denied")),
		util.Must(digest.NewInstanceName("unavailable1")),
		util.Must(digest.NewInstanceName("unavailable2")),
		util.Must(digest.NewInstanceName("unavailable3")),
	}).Return([]error{
		nil,
		status.Error(codes.PermissionDenied, "1: Permission denied for ok2"),
		status.Error(codes.PermissionDenied, "1: Permission denied for ok3"),
		status.Error(codes.PermissionDenied, "1: Permission denied for denied"),
		status.Error(codes.Unavailable, "1: Server offline for unavailable1"),
		status.Error(codes.PermissionDenied, "1: Permission denied for unavailable2"),
		status.Error(codes.PermissionDenied, "1: Permission denied for unavailable3"),
	})
	baseAuthorizer2.EXPECT().Authorize(ctx, []digest.InstanceName{
		util.Must(digest.NewInstanceName("ok2")),
		util.Must(digest.NewInstanceName("ok3")),
		util.Must(digest.NewInstanceName("denied")),
		util.Must(digest.NewInstanceName("unavailable2")),
		util.Must(digest.NewInstanceName("unavailable3")),
	}).Return([]error{
		nil,
		status.Error(codes.PermissionDenied, "2: Permission denied for ok3"),
		status.Error(codes.PermissionDenied, "2: Permission denied for denied"),
		status.Error(codes.Unavailable, "2: Server offline for unavailable2"),
		status.Error(codes.PermissionDenied, "2: Permission denied for unavailable3"),
	})
	baseAuthorizer3.EXPECT().Authorize(ctx, []digest.InstanceName{
		util.Must(digest.NewInstanceName("ok3")),
		util.Must(digest.NewInstanceName("denied")),
		util.Must(digest.NewInstanceName("unavailable3")),
	}).Return([]error{
		nil,
		status.Error(codes.PermissionDenied, "3: Permission denied for denied"),
		status.Error(codes.Unavailable, "3: Server offline for unavailable3"),
	})

	errs := authorizer.Authorize(ctx, []digest.InstanceName{
		util.Must(digest.NewInstanceName("ok1")),
		util.Must(digest.NewInstanceName("ok2")),
		util.Must(digest.NewInstanceName("ok3")),
		util.Must(digest.NewInstanceName("denied")),
		util.Must(digest.NewInstanceName("unavailable1")),
		util.Must(digest.NewInstanceName("unavailable2")),
		util.Must(digest.NewInstanceName("unavailable3")),
	})
	require.Len(t, errs, 7)
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.NoError(t, errs[2])
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "1: Permission denied for denied"), errs[3])
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "1: Server offline for unavailable1"), errs[4])
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "2: Server offline for unavailable2"), errs[5])
	testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "3: Server offline for unavailable3"), errs[6])
}
