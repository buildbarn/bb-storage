package auth_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestJMESPathExpressionAuthorizer(t *testing.T) {
	a := auth.NewJMESPathExpressionAuthorizer(jmespath.MustCompile("contains(authenticationMetadata.permittedInstanceNames, instanceName)"))

	instanceNames := []digest.InstanceName{
		digest.MustNewInstanceName("allowed"),
		digest.MustNewInstanceName("forbidden"),
	}

	t.Run("NoAuthenticationMetadata", func(t *testing.T) {
		// If no metadata is present, requests are denied.
		ctx := context.Background()
		errs := a.Authorize(ctx, instanceNames)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])
	})

	t.Run("EmptyAuthenticationMetadata", func(t *testing.T) {
		// The authentication metadata does not include the
		// "permittedInstanceNames" field.
		ctx := context.WithValue(context.Background(), auth.AuthenticationMetadata{}, nil)
		errs := a.Authorize(ctx, instanceNames)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])
	})

	t.Run("ValidAuthenticationMetadata", func(t *testing.T) {
		// The authentication metadata includes a
		// "permittedInstanceNames" field that gives access to the
		// "allowed" instance name.
		ctx := context.WithValue(context.Background(), auth.AuthenticationMetadata{}, map[string]interface{}{
			"permittedInstanceNames": []interface{}{"allowed"},
		})
		errs := a.Authorize(ctx, instanceNames)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])
	})
}
