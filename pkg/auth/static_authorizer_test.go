package auth_test

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStaticAuthorizer(t *testing.T) {
	matcher := func(in digest.InstanceName) bool { return strings.Contains(in.String(), "allowed") }
	a := auth.NewStaticAuthorizer(matcher)

	errs := a.Authorize(context.Background(), []digest.InstanceName{util.Must(digest.NewInstanceName("allowed"))})
	require.NoError(t, errs[0])

	errs = a.Authorize(context.Background(), []digest.InstanceName{util.Must(digest.NewInstanceName("allowed")), util.Must(digest.NewInstanceName("other-allowed"))})
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	errs = a.Authorize(context.Background(), []digest.InstanceName{util.Must(digest.NewInstanceName("forbidden"))})
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])

	errs = a.Authorize(context.Background(), []digest.InstanceName{util.Must(digest.NewInstanceName("allowed")), util.Must(digest.NewInstanceName("forbidden"))})
	require.NoError(t, errs[0])
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])

	errs = a.Authorize(context.Background(), []digest.InstanceName{util.Must(digest.NewInstanceName("forbidden")), util.Must(digest.NewInstanceName("allowed"))})
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])
	require.NoError(t, errs[1])
}
