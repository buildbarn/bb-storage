package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
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
	a := bb_grpc.NewAllowAuthenticator(expectedMetadata)
	actualMetadata, err := a.Authenticate(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedMetadata, actualMetadata)
}
