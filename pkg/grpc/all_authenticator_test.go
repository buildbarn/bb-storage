package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/proto/otlp/common/v1"

	"go.uber.org/mock/gomock"
)

func TestAllAuthenticatorZero(t *testing.T) {
	var wantMetadata auth_pb.AuthenticationMetadata

	a := bb_grpc.NewAllAuthenticator(nil)

	metadata, err := a.Authenticate(context.Background())
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &wantMetadata, metadata.GetFullProto())
}

func TestAllAuthenticatorMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	m0 := mock.NewMockGRPCAuthenticator(ctrl)
	m1 := mock.NewMockGRPCAuthenticator(ctrl)
	a := bb_grpc.NewAllAuthenticator([]bb_grpc.Authenticator{m0, m1})

	t.Run("FirstFailure", func(t *testing.T) {
		// There is no need to check the other authentication
		// backends if the first already returns failure.
		m0.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No token present"))

		_, err := a.Authenticate(ctx)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No token present"), err)
	})

	t.Run("SecondFailure", func(t *testing.T) {
		// There is no need to check the other authentication
		// backends if the first already returns failure.
		m0.EXPECT().Authenticate(ctx).Return(auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("You're totally who you say you are"),
		}), nil)
		m1.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No token present"))

		_, err := a.Authenticate(ctx)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No token present"), err)
	})

	t.Run("BothSuccess", func(t *testing.T) {
		wantMergedPublicStruct, err := structpb.NewStruct(map[string]any{
			"from0":   "You're totally who you say you are",
			"from1":   "You're maybe who you say you are",
			"decider": "m1",
		})
		require.NoError(t, err)

		wantPublicProto := &auth_pb.AuthenticationMetadata{
			Public: structpb.NewStructValue(wantMergedPublicStruct),
		}

		privateStruct0, err := structpb.NewStruct(map[string]any{
			"s0l0":      []any{"s0l00"},
			"s0l1":      []any{0},
			"s0i":       1,
			"both-int":  123,
			"both-list": []any{"both0"},
		})
		require.NoError(t, err)

		privateStruct1, err := structpb.NewStruct(map[string]any{
			"s1l0":      []any{"s1l00"},
			"s1l1":      []any{"one"},
			"s1i":       2,
			"both-int":  456,
			"both-list": []any{"both1"},
		})
		require.NoError(t, err)

		publicStruct0, err := structpb.NewStruct(map[string]any{
			"from0":   "You're totally who you say you are",
			"decider": "m0",
		})
		require.NoError(t, err)

		publicStruct1, err := structpb.NewStruct(map[string]any{
			"from1":   "You're maybe who you say you are",
			"decider": "m1",
		})
		require.NoError(t, err)

		m0.EXPECT().Authenticate(ctx).Return(auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public:  structpb.NewStructValue(publicStruct0),
			Private: structpb.NewStructValue(privateStruct0),
			TracingAttributes: []*v1.KeyValue{
				{
					Key: "foo",
					Value: &v1.AnyValue{
						Value: &v1.AnyValue_StringValue{
							StringValue: "hello",
						},
					},
				},
			},
		}), nil)
		m1.EXPECT().Authenticate(ctx).Return(auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public:  structpb.NewStructValue(publicStruct1),
			Private: structpb.NewStructValue(privateStruct1),
			TracingAttributes: []*v1.KeyValue{
				{
					Key: "bar",
					Value: &v1.AnyValue{
						Value: &v1.AnyValue_StringValue{
							StringValue: "goodbye",
						},
					},
				},
			},
		}), nil)
		metadata, err := a.Authenticate(ctx)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"private": map[string]any{
				"s0l0":      []any{"s0l00"},
				"s0l1":      []any{float64(0)},
				"s1l0":      []any{"s1l00"},
				"s1l1":      []any{"one"},
				"s0i":       float64(1),
				"s1i":       float64(2),
				"both-int":  float64(456),
				"both-list": []any{"both1"},
			},
			"public": map[string]any{
				"from0":   "You're totally who you say you are",
				"from1":   "You're maybe who you say you are",
				"decider": "m1",
			},
			"tracingAttributes": []any{
				map[string]any{
					"key": "foo",
					"value": map[string]any{
						"stringValue": "hello",
					},
				},
				map[string]any{
					"key": "bar",
					"value": map[string]any{
						"stringValue": "goodbye",
					},
				},
			},
		}, metadata.GetRaw())
		gotPublicProto, _ := metadata.GetPublicProto()
		testutil.RequireEqualProto(t, wantPublicProto, gotPublicProto)
		require.Equal(t, []attribute.KeyValue{
			{
				Key:   "auth.foo",
				Value: attribute.StringValue("hello"),
			},
			{
				Key:   "auth.bar",
				Value: attribute.StringValue("goodbye"),
			},
		}, metadata.GetTracingAttributes())
	})
}
