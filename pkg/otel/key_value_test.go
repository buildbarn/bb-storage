package otel_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/proto/otlp/common/v1"
)

func TestNewKeyValueListFromProto(t *testing.T) {
	t.Run("NoValue", func(t *testing.T) {
		// Providing no value should not cause any crashes.
		_, err := otel.NewKeyValueListFromProto([]*v1.KeyValue{
			{Key: "no_value"},
		}, "")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attribute \"no_value\" is of an unknown type"), err)
	})

	t.Run("Bytes", func(t *testing.T) {
		// Even though bytes attributes are part of the
		// protocol, the SDK does not allow instantiating them.
		_, err := otel.NewKeyValueListFromProto([]*v1.KeyValue{
			{
				Key: "unsupported",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_BytesValue{
						BytesValue: []byte("Hello"),
					},
				},
			},
		}, "")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attribute \"unsupported\" is of an unknown type"), err)
	})

	t.Run("HeterogeneousList", func(t *testing.T) {
		// The SDK requires that lists are homogeneous (e.g.,
		// only consisting of booleans), even though the wire
		// format allows them to be heterogeneous.
		_, err := otel.NewKeyValueListFromProto([]*v1.KeyValue{
			{
				Key: "heterogeneous",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_ArrayValue{
						ArrayValue: &v1.ArrayValue{
							Values: []*v1.AnyValue{
								{
									Value: &v1.AnyValue_BoolValue{
										BoolValue: false,
									},
								},
								{
									Value: &v1.AnyValue_IntValue{
										IntValue: 37,
									},
								},
							},
						},
					},
				},
			},
		}, "")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attribute \"heterogeneous\" is not a homogeneous list"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Convert a Protobuf message that contains each of the
		// output types once.
		keyValues, err := otel.NewKeyValueListFromProto([]*v1.KeyValue{
			{
				Key: "bool",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_BoolValue{
						BoolValue: true,
					},
				},
			},
			{
				Key: "int64",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_IntValue{
						IntValue: 42,
					},
				},
			},
			{
				Key: "float64",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_DoubleValue{
						DoubleValue: 7.5,
					},
				},
			},
			{
				Key: "string",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_StringValue{
						StringValue: "hello",
					},
				},
			},
			// Empty lists cannot be converted into an
			// attribute, as the element type cannot be
			// derived.
			{
				Key: "empty_slice",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_ArrayValue{
						ArrayValue: &v1.ArrayValue{},
					},
				},
			},
			{
				Key: "bool_slice",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_ArrayValue{
						ArrayValue: &v1.ArrayValue{
							Values: []*v1.AnyValue{
								{
									Value: &v1.AnyValue_BoolValue{
										BoolValue: false,
									},
								},
								{
									Value: &v1.AnyValue_BoolValue{
										BoolValue: true,
									},
								},
							},
						},
					},
				},
			},
			{
				Key: "int64_slice",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_ArrayValue{
						ArrayValue: &v1.ArrayValue{
							Values: []*v1.AnyValue{
								{
									Value: &v1.AnyValue_IntValue{
										IntValue: 13,
									},
								},
								{
									Value: &v1.AnyValue_IntValue{
										IntValue: 37,
									},
								},
							},
						},
					},
				},
			},
			{
				Key: "float64_slice",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_ArrayValue{
						ArrayValue: &v1.ArrayValue{
							Values: []*v1.AnyValue{
								{
									Value: &v1.AnyValue_DoubleValue{
										DoubleValue: 0.25,
									},
								},
								{
									Value: &v1.AnyValue_DoubleValue{
										DoubleValue: 0.5,
									},
								},
							},
						},
					},
				},
			},
			{
				Key: "string_slice",
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_ArrayValue{
						ArrayValue: &v1.ArrayValue{
							Values: []*v1.AnyValue{
								{
									Value: &v1.AnyValue_StringValue{
										StringValue: "hello",
									},
								},
								{
									Value: &v1.AnyValue_StringValue{
										StringValue: "world",
									},
								},
							},
						},
					},
				},
			},
		}, "prefix.")
		require.NoError(t, err)
		require.Equal(t, []attribute.KeyValue{
			attribute.Bool("prefix.bool", true),
			attribute.Int64("prefix.int64", 42),
			attribute.Float64("prefix.float64", 7.5),
			attribute.String("prefix.string", "hello"),
			attribute.BoolSlice("prefix.bool_slice", []bool{false, true}),
			attribute.Int64Slice("prefix.int64_slice", []int64{13, 37}),
			attribute.Float64Slice("prefix.float64_slice", []float64{0.25, 0.5}),
			attribute.StringSlice("prefix.string_slice", []string{"hello", "world"}),
		}, keyValues)
	})
}
