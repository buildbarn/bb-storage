package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"go.uber.org/mock/gomock"
)

func TestRequestHeadersAuthenticator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	md := metadata.New(
		map[string]string{
			"Authorization": "token",
			"OtherHeader":   "Other",
			"UnusedHeader":  "DontUseMe",
		},
	)
	md.Append("OtherHeader", "Other2")
	grpcCtx := metadata.NewIncomingContext(ctx, md)
	backend := mock.NewMockRequestHeadersAuthenticator(ctrl)

	t.Run("BackendSuccess", func(t *testing.T) {
		backend.EXPECT().Authenticate(
			grpcCtx, map[string][]string{
				"Authorization": {"token"},
				"OtherHEADER":   {"Other", "Other2"},
				// MissingHeader is not part of the request.
				// UnusedHeader should not be passed.
			},
		).Return(util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("You're totally who you say you are"),
		})), nil)

		authenticator := bb_grpc.NewRequestHeadersAuthenticator(
			backend,
			[]string{
				"Authorization",
				"OtherHEADER",
				"MissingHeader",
			},
		)
		metadata, err := authenticator.Authenticate(grpcCtx)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"public": "You're totally who you say you are",
		}, metadata.GetRaw())
	})

	t.Run("BackendFailure", func(t *testing.T) {
		headerKeys := []string{}
		backend.EXPECT().Authenticate(
			grpcCtx, map[string][]string{},
		).Return(nil, status.Error(codes.Unavailable, "Server offline"))

		authenticator := bb_grpc.NewRequestHeadersAuthenticator(
			backend,
			headerKeys,
		)
		_, err := authenticator.Authenticate(grpcCtx)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Server offline"),
			err)
	})
}
