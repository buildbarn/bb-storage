package otel

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/proto/otlp/common/v1"
)

// NewKeyValueListFromProto creates a list of attribute key-value pairs
// from the Protobuf message that's used by OpenTelemetry's gRPC wire
// format. The OpenTelemetry SDK for Go does not provide any publicly
// usable utility functions for this.
func NewKeyValueListFromProto(inList []*v1.KeyValue, keyPrefix string) ([]attribute.KeyValue, error) {
	outList := make([]attribute.KeyValue, 0, len(inList))
	for _, in := range inList {
		key := keyPrefix + in.Key
		switch value := in.Value.GetValue().(type) {
		case *v1.AnyValue_BoolValue:
			outList = append(outList, attribute.Bool(key, value.BoolValue))
		case *v1.AnyValue_IntValue:
			outList = append(outList, attribute.Int64(key, value.IntValue))
		case *v1.AnyValue_DoubleValue:
			outList = append(outList, attribute.Float64(key, value.DoubleValue))
		case *v1.AnyValue_StringValue:
			outList = append(outList, attribute.String(key, value.StringValue))
		case *v1.AnyValue_ArrayValue:
			if elements := value.ArrayValue.Values; len(elements) > 0 {
				switch elements[0].Value.(type) {
				case *v1.AnyValue_BoolValue:
					elementValues := make([]bool, 0, len(elements))
					for _, element := range elements {
						elementValue, ok := element.Value.(*v1.AnyValue_BoolValue)
						if !ok {
							return nil, status.Errorf(codes.InvalidArgument, "Attribute %#v is not a homogeneous list", in.Key)
						}
						elementValues = append(elementValues, elementValue.BoolValue)
					}
					outList = append(outList, attribute.BoolSlice(key, elementValues))
				case *v1.AnyValue_IntValue:
					elementValues := make([]int64, 0, len(elements))
					for _, element := range elements {
						elementValue, ok := element.Value.(*v1.AnyValue_IntValue)
						if !ok {
							return nil, status.Errorf(codes.InvalidArgument, "Attribute %#v is not a homogeneous list", in.Key)
						}
						elementValues = append(elementValues, elementValue.IntValue)
					}
					outList = append(outList, attribute.Int64Slice(key, elementValues))
				case *v1.AnyValue_DoubleValue:
					elementValues := make([]float64, 0, len(elements))
					for _, element := range elements {
						elementValue, ok := element.Value.(*v1.AnyValue_DoubleValue)
						if !ok {
							return nil, status.Errorf(codes.InvalidArgument, "Attribute %#v is not a homogeneous list", in.Key)
						}
						elementValues = append(elementValues, elementValue.DoubleValue)
					}
					outList = append(outList, attribute.Float64Slice(key, elementValues))
				case *v1.AnyValue_StringValue:
					elementValues := make([]string, 0, len(elements))
					for _, element := range elements {
						elementValue, ok := element.Value.(*v1.AnyValue_StringValue)
						if !ok {
							return nil, status.Errorf(codes.InvalidArgument, "Attribute %#v is not a homogeneous list", in.Key)
						}
						elementValues = append(elementValues, elementValue.StringValue)
					}
					outList = append(outList, attribute.StringSlice(key, elementValues))
				default:
					return nil, status.Errorf(codes.InvalidArgument, "First element of attribute %#v is of an unknown type", in.Key)
				}
			}
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Attribute %#v is of an unknown type", in.Key)
		}
	}
	return outList, nil
}
