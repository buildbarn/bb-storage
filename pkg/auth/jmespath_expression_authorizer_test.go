package auth_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestJMESPathExpressionAuthorizer(t *testing.T) {
	a := auth.NewJMESPathExpressionAuthorizer(jmespath.MustCompile("contains(authenticationMetadata.private.permittedInstanceNames, instanceName)"))

	instanceNames := []digest.InstanceName{
		util.Must(digest.NewInstanceName("allowed")),
		util.Must(digest.NewInstanceName("forbidden")),
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
		ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{})))
		errs := a.Authorize(ctx, instanceNames)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])
	})

	t.Run("ValidAuthenticationMetadata", func(t *testing.T) {
		// The authentication metadata includes a
		// "permittedInstanceNames" field that gives access to the
		// "allowed" instance name.
		ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Private: structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"permittedInstanceNames": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("allowed"),
						},
					}),
				},
			}),
		})))
		errs := a.Authorize(ctx, instanceNames)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Permission denied"), errs[1])
	})
}
