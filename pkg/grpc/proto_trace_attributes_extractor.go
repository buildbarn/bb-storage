package grpc

import (
	"context"
	"encoding/base64"
	"math"
	"strconv"
	"strings"
	"sync"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ProtoTraceAttributesExtractor is a gRPC client and server interceptor
// that can be used to attach attributes to the trace spans created by
// the OpenTelemetry gRPC interceptors ("otelgrpc"), whose values are
// based on RPC request and response payloads.
type ProtoTraceAttributesExtractor struct {
	methods map[string]*methodTraceAttributesExtractor
}

// NewProtoTraceAttributesExtractor creates a new
// ProtoTraceAttributesExtractor that captures fields from request and
// response payloads for the RPCs that are provided in the
// configuration.
func NewProtoTraceAttributesExtractor(configuration map[string]*configuration.TracingMethodConfiguration, errorLogger util.ErrorLogger) *ProtoTraceAttributesExtractor {
	pe := &ProtoTraceAttributesExtractor{
		methods: make(map[string]*methodTraceAttributesExtractor, len(configuration)),
	}
	for methodName, methodConfiguration := range configuration {
		pe.methods[methodName] = &methodTraceAttributesExtractor{
			errorLogger:        errorLogger,
			requestAttributes:  methodConfiguration.AttributesFromFirstRequestMessage,
			responseAttributes: methodConfiguration.AttributesFromFirstResponseMessage,
		}
	}
	return pe
}

func recordingSpanFromContext(ctx context.Context) trace.Span {
	if span := trace.SpanFromContext(ctx); span != nil && span.IsRecording() {
		return span
	}
	return nil
}

// InterceptUnaryClient is a gRPC unary client interceptor that attaches
// attributes to trace spans created by the OpenTelemetry gRPC
// unary client interceptor.
func (pe *ProtoTraceAttributesExtractor) InterceptUnaryClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	me, ok := pe.methods[method]
	if !ok {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	span := recordingSpanFromContext(ctx)
	if span == nil {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	me.applyAttributes(span, me.requestAttributesFor(req))
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err == nil {
		me.applyAttributes(span, me.responseAttributesFor(reply))
	}
	return err
}

var _ grpc.UnaryClientInterceptor = (&ProtoTraceAttributesExtractor{}).InterceptUnaryClient

// InterceptStreamClient is a gRPC stream client interceptor that
// attaches attributes to trace spans created by the OpenTelemetry gRPC
// stream client interceptor.
func (pe *ProtoTraceAttributesExtractor) InterceptStreamClient(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	me, ok := pe.methods[method]
	if !ok {
		return streamer(ctx, desc, cc, method, opts...)
	}
	span := recordingSpanFromContext(ctx)
	if span == nil {
		return streamer(ctx, desc, cc, method, opts...)
	}
	clientStream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	return &attributeExtractingClientStream{
		ClientStream: clientStream,
		method:       me,
		span:         span,
	}, nil
}

var _ grpc.StreamClientInterceptor = (&ProtoTraceAttributesExtractor{}).InterceptStreamClient

// InterceptUnaryServer is a gRPC unary server interceptor that attaches
// attributes to trace spans created by the OpenTelemetry gRPC
// unary server interceptor.
func (pe *ProtoTraceAttributesExtractor) InterceptUnaryServer(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	me, ok := pe.methods[info.FullMethod]
	if !ok {
		return handler(ctx, req)
	}
	span := recordingSpanFromContext(ctx)
	if span == nil {
		return handler(ctx, req)
	}
	me.applyAttributes(span, me.requestAttributesFor(req))
	resp, err := handler(ctx, req)
	if err == nil {
		me.applyAttributes(span, me.responseAttributesFor(resp))
	}
	return resp, err
}

var _ grpc.UnaryServerInterceptor = (&ProtoTraceAttributesExtractor{}).InterceptUnaryServer

// InterceptStreamServer is a gRPC stream server interceptor that
// attaches attributes to trace spans created by the OpenTelemetry gRPC
// stream server interceptor.
func (pe *ProtoTraceAttributesExtractor) InterceptStreamServer(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	me, ok := pe.methods[info.FullMethod]
	if !ok {
		return handler(srv, ss)
	}
	span := recordingSpanFromContext(ss.Context())
	if span == nil {
		return handler(srv, ss)
	}
	return handler(srv, &attributeExtractingServerStream{
		ServerStream: ss,
		method:       me,
		span:         span,
	})
}

var _ grpc.StreamServerInterceptor = (&ProtoTraceAttributesExtractor{}).InterceptStreamServer

// methodTraceAttributesExtractor is the bookkeeping that needs to be
// tracked by ProtoTraceAttributesExtractor per gRPC method.
type methodTraceAttributesExtractor struct {
	errorLogger util.ErrorLogger

	requestAttributes []string
	requestOnce       sync.Once
	requestExtractor  directionTraceAttributesExtractor

	responseAttributes []string
	responseOnce       sync.Once
	responseExtractor  directionTraceAttributesExtractor
}

func (me *methodTraceAttributesExtractor) requestAttributesFor(req interface{}) []attribute.KeyValue {
	me.requestOnce.Do(func() {
		// First time we see an RPC message going from the
		// client to the server.
		me.requestExtractor.initialize("request", me.requestAttributes, req, me.errorLogger)
	})
	return me.requestExtractor.extractAttributes(req)
}

func (me *methodTraceAttributesExtractor) responseAttributesFor(resp interface{}) []attribute.KeyValue {
	me.responseOnce.Do(func() {
		// First time we see an RPC message going from the
		// server to the client.
		me.responseExtractor.initialize("response", me.responseAttributes, resp, me.errorLogger)
	})
	return me.responseExtractor.extractAttributes(resp)
}

// methodTraceAttributesExtractor is the bookkeeping that needs to be
// tracked by ProtoTraceAttributesExtractor per gRPC method's direction
// (i.e., request or response).
type directionTraceAttributesExtractor struct {
	attributeExtractors []attributeExtractor
}

func (de *directionTraceAttributesExtractor) initialize(attributePrefix string, attributes []string, m interface{}, errorLogger util.ErrorLogger) {
	// Construct attribute extractor functions for each of the
	// attributes provided in the configuration. This prevents the
	// need to analyze the reflection data during subsequent RPCs.
	descriptor := m.(proto.Message).ProtoReflect().Descriptor()
	for _, attribute := range attributes {
		fields := strings.FieldsFunc(attribute, func(c rune) bool { return c == '.' })
		fullAttributeName := strings.Join(append([]string{attributePrefix}, fields...), ".")
		if attributeExtractor, err := newAttributeExtractor(descriptor, fields, fullAttributeName); err == nil {
			de.attributeExtractors = append(de.attributeExtractors, attributeExtractor)
		} else {
			errorLogger.Log(util.StatusWrapf(err, "Failed to create extractor for attribute %#v", fullAttributeName))
		}
	}
}

func (de *directionTraceAttributesExtractor) extractAttributes(m interface{}) []attribute.KeyValue {
	if len(de.attributeExtractors) == 0 {
		return nil
	}
	mProtoReflect := m.(proto.Message).ProtoReflect()
	attributes := make([]attribute.KeyValue, 0, len(de.attributeExtractors))
	for _, attributeExtractor := range de.attributeExtractors {
		attributes = attributeExtractor(mProtoReflect, attributes)
	}
	return attributes
}

// attributeExtractor is a function type that is capable of extracting a
// single field from a Protobuf message and convert it to an
// OpenTelemetry attribute of the same type.
type attributeExtractor func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue

func newAttributeExtractor(descriptor protoreflect.MessageDescriptor, remainingFields []string, fullAttributeName string) (attributeExtractor, error) {
	if len(remainingFields) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Attribute name does not contain any fields")
	}
	fieldName := remainingFields[0]
	fieldDescriptor := descriptor.Fields().ByTextName(fieldName)
	if fieldDescriptor == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Field %#v does not exist", fieldName)
	}

	if len(remainingFields) > 1 {
		// Field is stored in a nested message. Only report this
		// attribute if the containing message is set.
		if fieldDescriptor.Kind() != protoreflect.MessageKind || fieldDescriptor.Cardinality() == protoreflect.Repeated {
			return nil, status.Errorf(codes.InvalidArgument, "Field %#v does not refer to a singular message", fieldName)
		}

		nestedExtractor, err := newAttributeExtractor(fieldDescriptor.Message(), remainingFields[1:], fullAttributeName)
		if err != nil {
			return nil, err
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			if !m.Has(fieldDescriptor) {
				return attributes
			}
			return nestedExtractor(m.Get(fieldDescriptor).Message(), attributes)
		}, nil
	}

	if fieldDescriptor.IsMap() {
		return nil, status.Errorf(codes.InvalidArgument, "Field %#v refers to a map, while only singular and repeated fields are supported", fieldName)
	}

	switch fieldDescriptor.Kind() {
	case protoreflect.BoolKind:
		// Boolean or repeated boolean field.
		if fieldDescriptor.IsList() {
			return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
				list := m.Get(fieldDescriptor).List()
				length := list.Len()
				elements := make([]bool, 0, length)
				for i := 0; i < length; i++ {
					elements = append(elements, list.Get(i).Bool())
				}
				return append(attributes, attribute.BoolSlice(fullAttributeName, elements))
			}, nil
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			return append(attributes, attribute.Bool(fullAttributeName, m.Get(fieldDescriptor).Bool()))
		}, nil
	case protoreflect.EnumKind:
		// Enumeration or repeated enumeration field. Convert
		// these to a string or string array type.
		enumValuesDescriptor := fieldDescriptor.Enum().Values()
		enumValuesLength := enumValuesDescriptor.Len()
		enumValuesByNumber := make(map[protoreflect.EnumNumber]string, enumValuesLength)
		for i := 0; i < enumValuesLength; i++ {
			enumValue := enumValuesDescriptor.Get(i)
			enumValuesByNumber[enumValue.Number()] = string(enumValue.Name())
		}
		convertEnumNumberToString := func(value protoreflect.Value) string {
			number := value.Enum()
			if label, ok := enumValuesByNumber[number]; ok {
				// Known enumeration value.
				return label
			}
			// Unknown numeration value. Use decimal representation.
			return strconv.FormatInt(int64(number), 10)
		}

		if fieldDescriptor.IsList() {
			return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
				list := m.Get(fieldDescriptor).List()
				length := list.Len()
				elements := make([]string, 0, length)
				for i := 0; i < length; i++ {
					elements = append(elements, convertEnumNumberToString(list.Get(i)))
				}
				return append(attributes, attribute.StringSlice(fullAttributeName, elements))
			}, nil
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			return append(attributes, attribute.String(fullAttributeName, convertEnumNumberToString(m.Get(fieldDescriptor))))
		}, nil
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		// Floating point or repeated floating point field.
		if fieldDescriptor.IsList() {
			return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
				list := m.Get(fieldDescriptor).List()
				length := list.Len()
				elements := make([]float64, 0, length)
				for i := 0; i < length; i++ {
					elements = append(elements, list.Get(i).Float())
				}
				return append(attributes, attribute.Float64Slice(fullAttributeName, elements))
			}, nil
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			return append(attributes, attribute.Float64(fullAttributeName, m.Get(fieldDescriptor).Float()))
		}, nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		// Signed integer or repeated signed integer field.
		if fieldDescriptor.IsList() {
			return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
				list := m.Get(fieldDescriptor).List()
				length := list.Len()
				elements := make([]int64, 0, length)
				for i := 0; i < length; i++ {
					elements = append(elements, list.Get(i).Int())
				}
				return append(attributes, attribute.Int64Slice(fullAttributeName, elements))
			}, nil
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			return append(attributes, attribute.Int64(fullAttributeName, m.Get(fieldDescriptor).Int()))
		}, nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		// Unsigned integer or repeated unsigned integer field.
		convertUnsignedToInt64 := func(value protoreflect.Value) int64 {
			unsigned := value.Uint()
			if unsigned > uint64(math.MaxInt64) {
				return math.MaxInt64
			}
			return int64(unsigned)
		}
		if fieldDescriptor.IsList() {
			return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
				list := m.Get(fieldDescriptor).List()
				length := list.Len()
				elements := make([]int64, 0, length)
				for i := 0; i < length; i++ {
					elements = append(elements, convertUnsignedToInt64(list.Get(i)))
				}
				return append(attributes, attribute.Int64Slice(fullAttributeName, elements))
			}, nil
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			return append(attributes, attribute.Int64(fullAttributeName, convertUnsignedToInt64(m.Get(fieldDescriptor))))
		}, nil
	case protoreflect.BytesKind:
		// Bytes or repeated bytes field.
		if fieldDescriptor.IsList() {
			return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
				list := m.Get(fieldDescriptor).List()
				length := list.Len()
				elements := make([]string, 0, length)
				for i := 0; i < length; i++ {
					elements = append(elements, base64.StdEncoding.EncodeToString(list.Get(i).Bytes()))
				}
				return append(attributes, attribute.StringSlice(fullAttributeName, elements))
			}, nil
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			return append(attributes, attribute.String(fullAttributeName, base64.StdEncoding.EncodeToString(m.Get(fieldDescriptor).Bytes())))
		}, nil
	case protoreflect.StringKind:
		// String or repeated string field.
		if fieldDescriptor.IsList() {
			return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
				list := m.Get(fieldDescriptor).List()
				length := list.Len()
				elements := make([]string, 0, length)
				for i := 0; i < length; i++ {
					elements = append(elements, list.Get(i).String())
				}
				return append(attributes, attribute.StringSlice(fullAttributeName, elements))
			}, nil
		}
		return func(m protoreflect.Message, attributes []attribute.KeyValue) []attribute.KeyValue {
			return append(attributes, attribute.String(fullAttributeName, m.Get(fieldDescriptor).String()))
		}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Field %#v does not have a boolean, enumeration, floating point, integer, bytes or string type", fieldName)
	}
}

// attributeExtractingClientStream is a decorator for grpc.ClientStream
// that extracts trace span attributes from the first request and
// response message in a streaming RPC.
type attributeExtractingClientStream struct {
	grpc.ClientStream
	method           *methodTraceAttributesExtractor
	span             trace.Span
	gotFirstRequest  bool
	gotFirstResponse bool
	requestIndex     uint64
	responseIndex    uint64
}

func (cs *attributeExtractingClientStream) SendMsg(m interface{}) error {
	attributes := cs.method.requestAttributesFor(m)
	if !cs.gotFirstRequest {
		cs.gotFirstRequest = true
		cs.method.applyAttributes(cs.span, attributes)
	}
	err := cs.ClientStream.SendMsg(m)
	if err == nil {
		cs.requestIndex++
		addMessageEvent(cs.span, "out", cs.requestIndex, attributes)
	}
	return err
}

func (cs *attributeExtractingClientStream) RecvMsg(m interface{}) error {
	if !cs.gotFirstResponse {
		if err := cs.ClientStream.RecvMsg(m); err != nil {
			return err
		}
		cs.gotFirstResponse = true
		attributes := cs.method.responseAttributesFor(m)
		cs.method.applyAttributes(cs.span, attributes)
		cs.responseIndex++
		addMessageEvent(cs.span, "in", cs.responseIndex, attributes)
		return nil
	}
	if err := cs.ClientStream.RecvMsg(m); err != nil {
		return err
	}
	cs.responseIndex++
	addMessageEvent(cs.span, "in", cs.responseIndex, cs.method.responseAttributesFor(m))
	return nil
}

// attributeExtractingServerStream is a decorator for grpc.ServerStream
// that extracts trace span attributes from the first request and
// response message in a streaming RPC.
type attributeExtractingServerStream struct {
	grpc.ServerStream
	method           *methodTraceAttributesExtractor
	span             trace.Span
	gotFirstRequest  bool
	gotFirstResponse bool
	requestIndex     uint64
	responseIndex    uint64
}

func (cs *attributeExtractingServerStream) RecvMsg(m interface{}) error {
	if !cs.gotFirstRequest {
		if err := cs.ServerStream.RecvMsg(m); err != nil {
			return err
		}
		cs.gotFirstRequest = true
		attributes := cs.method.requestAttributesFor(m)
		cs.method.applyAttributes(cs.span, attributes)
		cs.requestIndex++
		addMessageEvent(cs.span, "in", cs.requestIndex, attributes)
		return nil
	}
	if err := cs.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	cs.requestIndex++
	addMessageEvent(cs.span, "in", cs.requestIndex, cs.method.requestAttributesFor(m))
	return nil
}

func (cs *attributeExtractingServerStream) SendMsg(m interface{}) error {
	attributes := cs.method.responseAttributesFor(m)
	if !cs.gotFirstResponse {
		cs.gotFirstResponse = true
		cs.method.applyAttributes(cs.span, attributes)
	}
	err := cs.ServerStream.SendMsg(m)
	if err == nil {
		cs.responseIndex++
		addMessageEvent(cs.span, "out", cs.responseIndex, attributes)
	}
	return err
}

func (me *methodTraceAttributesExtractor) applyAttributes(span trace.Span, attributes []attribute.KeyValue) {
	if span == nil || !span.IsRecording() || len(attributes) == 0 {
		return
	}
	span.SetAttributes(attributes...)
}

func addMessageEvent(span trace.Span, direction string, index uint64, attributes []attribute.KeyValue) {
	if span == nil || !span.IsRecording() {
		return
	}
	attributes = append(attributes,
		attribute.String("grpc.message.direction", direction),
		attribute.Int64("grpc.message.index", int64(index)),
	)
	span.AddEvent("grpc.message", trace.WithAttributes(attributes...))
}
