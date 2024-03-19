package grpcauth_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/grpcauth"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestAllowAuthenticator(t *testing.T) {
	expectedMetadata := auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"username": structpb.NewStringValue("John Doe"),
			},
		}),
	})
	a := grpcauth.NewAllowAuthenticator(expectedMetadata)
	actualMetadata, err := a.Authenticate(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedMetadata, actualMetadata)
}
