package auth_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/structpb"

	"go.opentelemetry.io/otel/attribute"
)

func TestAuthenticationMetadata(t *testing.T) {
	t.Run("Nil", func(t *testing.T) {
		m, err := auth.NewAuthenticationMetadata(nil)
		require.NoError(t, err)

		require.Nil(t, m.GetRaw())

		publicProto, shouldDisplay := m.GetPublicProto()
		testutil.RequireEqualProto(t, &auth_pb.AuthenticationMetadata{}, publicProto)
		require.False(t, shouldDisplay)

		require.Empty(t, m.GetTracingAttributes())
	})

	t.Run("String", func(t *testing.T) {
		m, err := auth.NewAuthenticationMetadata("Hello")
		require.NoError(t, err)

		require.Equal(t, "Hello", m.GetRaw())

		publicProto, shouldDisplay := m.GetPublicProto()
		testutil.RequireEqualProto(t, &auth_pb.AuthenticationMetadata{}, publicProto)
		require.False(t, shouldDisplay)

		require.Empty(t, m.GetTracingAttributes())
	})

	t.Run("PublicNull", func(t *testing.T) {
		m, err := auth.NewAuthenticationMetadata(map[string]any{
			"public": nil,
		})
		require.NoError(t, err)

		require.Equal(t, map[string]any{
			"public": nil,
		}, m.GetRaw())

		publicProto, shouldDisplay := m.GetPublicProto()
		testutil.RequireEqualProto(t, &auth_pb.AuthenticationMetadata{
			Public: structpb.NewNullValue(),
		}, publicProto)
		require.True(t, shouldDisplay)

		require.Empty(t, m.GetTracingAttributes())
	})

	t.Run("PublicNonNull", func(t *testing.T) {
		m, err := auth.NewAuthenticationMetadata(map[string]any{
			"private": "top-secret",
			"public": map[string]any{
				"integer": 123,
				"string":  "foo",
				"list": []any{
					7.5,
					false,
					"bar",
				},
			},
		})
		require.NoError(t, err)

		require.Equal(t, map[string]any{
			"private": "top-secret",
			"public": map[string]any{
				"integer": 123,
				"string":  "foo",
				"list": []any{
					7.5,
					false,
					"bar",
				},
			},
		}, m.GetRaw())

		publicProto, shouldDisplay := m.GetPublicProto()
		testutil.RequireEqualProto(t, &auth_pb.AuthenticationMetadata{
			Public: structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"integer": structpb.NewNumberValue(123),
					"string":  structpb.NewStringValue("foo"),
					"list": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewNumberValue(7.5),
							structpb.NewBoolValue(false),
							structpb.NewStringValue("bar"),
						},
					}),
				},
			}),
		}, publicProto)
		require.True(t, shouldDisplay)

		require.Empty(t, m.GetTracingAttributes())
	})

	t.Run("TracingAttributes", func(t *testing.T) {
		m, err := auth.NewAuthenticationMetadata(map[string]any{
			"tracingAttributes": []any{
				map[string]any{
					"key": "username",
					"value": map[string]any{
						"stringValue": "john_doe",
					},
				},
			},
		})
		require.NoError(t, err)

		require.Equal(t, map[string]any{
			"tracingAttributes": []any{
				map[string]any{
					"key": "username",
					"value": map[string]any{
						"stringValue": "john_doe",
					},
				},
			},
		}, m.GetRaw())

		publicProto, shouldDisplay := m.GetPublicProto()
		testutil.RequireEqualProto(t, &auth_pb.AuthenticationMetadata{}, publicProto)
		require.False(t, shouldDisplay)

		require.Equal(t, []attribute.KeyValue{
			attribute.String("auth.username", "john_doe"),
		}, m.GetTracingAttributes())
	})
}
