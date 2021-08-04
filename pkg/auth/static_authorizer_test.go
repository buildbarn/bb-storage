package auth_test

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStaticAuthorizer(t *testing.T) {
	matcher := func(in digest.InstanceName) bool { return strings.Contains(in.String(), "allowed") }
	a := auth.NewStaticAuthorizer(matcher)

	errs := a.Authorize(context.Background(), []digest.InstanceName{digest.MustNewInstanceName("allowed")})
	require.NoError(t, errs[0])

	errs = a.Authorize(context.Background(), []digest.InstanceName{digest.MustNewInstanceName("allowed"), digest.MustNewInstanceName("other-allowed")})
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	errs = a.Authorize(context.Background(), []digest.InstanceName{digest.MustNewInstanceName("forbidden")})
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])

	errs = a.Authorize(context.Background(), []digest.InstanceName{digest.MustNewInstanceName("allowed"), digest.MustNewInstanceName("forbidden")})
	require.NoError(t, errs[0])
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])

	errs = a.Authorize(context.Background(), []digest.InstanceName{digest.MustNewInstanceName("forbidden"), digest.MustNewInstanceName("allowed")})
	testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])
	require.NoError(t, errs[1])
}
