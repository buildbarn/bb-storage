package grpc_test

import (
	"context"
	"encoding/base64"
	"reflect"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.uber.org/mock/gomock"
)

func hasAttribute(attributes []attribute.KeyValue, want attribute.KeyValue) bool {
	for _, attribute := range attributes {
		if reflect.DeepEqual(attribute, want) {
			return true
		}
	}
	return false
}

func findEventAttributes(t *testing.T, events []trace.EventConfig, direction string, index int64) []attribute.KeyValue {
	t.Helper()
	for _, event := range events {
		attributes := event.Attributes()
		if hasAttribute(attributes, attribute.String("grpc.message.direction", direction)) &&
			hasAttribute(attributes, attribute.Int64("grpc.message.index", index)) {
			return attributes
		}
	}
	t.Fatalf("event not found for direction=%q index=%d", direction, index)
	return nil
}

func TestProtoTraceAttributesExtractor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Common definitions used by the tests below.
	span := mock.NewMockSpan(ctrl)
	ctxWithSpan := trace.ContextWithSpan(ctx, span)

	t.Run("BytesAndUnsigned", func(t *testing.T) {
		fullMethod := "/buildbarn.fsac.FileSystemAccessCache/GetFileSystemAccessProfile"
		request := &fsac.GetFileSystemAccessProfileRequest{
			InstanceName:   "default-scheduler",
			DigestFunction: remoteexecution.DigestFunction_SHA256,
			ReducedActionDigest: &remoteexecution.Digest{
				Hash:      "abcd",
				SizeBytes: 123,
			},
		}
		response := &fsac.FileSystemAccessProfile{
			BloomFilter:              []byte{0x01, 0x02, 0x03},
			BloomFilterHashFunctions: 7,
		}

		extractor := bb_grpc.NewProtoTraceAttributesExtractor(map[string]*configuration.TracingMethodConfiguration{
			fullMethod: {
				AttributesFromFirstResponseMessage: []string{
					"bloom_filter",
					"bloom_filter_hash_functions",
				},
			},
		}, mock.NewMockErrorLogger(ctrl))

		handler := mock.NewMockUnaryHandler(ctrl)
		span.EXPECT().IsRecording().Return(true).AnyTimes()
		handler.EXPECT().Call(ctxWithSpan, request).Return(response, nil)
		span.EXPECT().SetAttributes([]attribute.KeyValue{
			attribute.String("response.bloom_filter", base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03})),
			attribute.Int64("response.bloom_filter_hash_functions", 7),
		})

		observedResponse, err := extractor.InterceptUnaryServer(ctxWithSpan, request, &grpc.UnaryServerInfo{
			FullMethod: fullMethod,
		}, handler.Call)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, response, observedResponse.(proto.Message))
	})
	exampleUnaryFullMethod := "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities"
	exampleUnaryRequest := &remoteexecution.GetCapabilitiesRequest{
		InstanceName: "default-scheduler",
	}
	exampleUnaryResponse := &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: []remoteexecution.DigestFunction_Value{
				remoteexecution.DigestFunction_MD5,
				remoteexecution.DigestFunction_SHA256,
			},
			MaxBatchTotalSizeBytes: 1 << 20,
		},
		ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
			ExecEnabled:             true,
			SupportedNodeProperties: []string{"CTime", "MTime"},
		},
	}

	errorLogger := mock.NewMockErrorLogger(ctrl)
	extractor := bb_grpc.NewProtoTraceAttributesExtractor(map[string]*configuration.TracingMethodConfiguration{
		exampleUnaryFullMethod: {
			AttributesFromFirstRequestMessage: []string{
				// Valid:
				"instance_name",

				// Invalid.
				"",
			},
			AttributesFromFirstResponseMessage: []string{
				// Valid.
				"cache_capabilities.digest_functions",
				"cache_capabilities.max_batch_total_size_bytes",
				"execution_capabilities.exec_enabled",
				"execution_capabilities.supported_node_properties",
				"low_api_version.major",

				// Invalid.
				"cache_capabilities",
				"nonexistent",
				"execution_capabilities.execution_priority_capabilities.priorities.min_priority",
			},
		},
	}, errorLogger)

	t.Run("InterceptUnaryClient", func(t *testing.T) {
		invoker := mock.NewMockUnaryInvoker(ctrl)

		t.Run("NoSpanInContext", func(t *testing.T) {
			// If there is no tracing span in the context,
			// the call should be forwarded in literal form.
			var observedResponse remoteexecution.ServerCapabilities
			invoker.EXPECT().Call(ctx, exampleUnaryFullMethod, exampleUnaryRequest, &observedResponse, nil).
				Do(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					proto.Merge(reply.(proto.Message), exampleUnaryResponse)
					return nil
				})

			require.NoError(t, extractor.InterceptUnaryClient(ctx, exampleUnaryFullMethod, exampleUnaryRequest, &observedResponse, nil, invoker.Call))
			testutil.RequireEqualProto(t, exampleUnaryResponse, &observedResponse)
		})

		t.Run("UnknownMethod", func(t *testing.T) {
			// Methods for which we've not specified a
			// configuration should have their calls
			// forwarded in literal form.
			request := &remoteexecution.UpdateActionResultRequest{
				InstanceName: "default-scheduler",
			}
			response := &remoteexecution.ActionResult{}
			fullMethod := "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult"

			var observedResponse remoteexecution.ActionResult
			invoker.EXPECT().Call(ctx, fullMethod, request, &observedResponse, nil).
				Do(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					proto.Merge(reply.(proto.Message), response)
					return nil
				})

			require.NoError(t, extractor.InterceptUnaryClient(ctx, fullMethod, request, &observedResponse, nil, invoker.Call))
			testutil.RequireEqualProto(t, response, &observedResponse)
		})

		t.Run("NonRecordingSpan", func(t *testing.T) {
			// If the span is not recording, we shouldn't
			// spend any effort inspecting the request and
			// response.
			span := mock.NewMockSpan(ctrl)
			ctxWithSpan := trace.ContextWithSpan(ctx, span)
			span.EXPECT().IsRecording().Return(false)
			var observedResponse remoteexecution.ServerCapabilities
			invoker.EXPECT().Call(ctxWithSpan, exampleUnaryFullMethod, exampleUnaryRequest, &observedResponse, nil).
				Do(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					proto.Merge(reply.(proto.Message), exampleUnaryResponse)
					return nil
				})

			require.NoError(t, extractor.InterceptUnaryClient(ctxWithSpan, exampleUnaryFullMethod, exampleUnaryRequest, &observedResponse, nil, invoker.Call))
			testutil.RequireEqualProto(t, exampleUnaryResponse, &observedResponse)
		})

		t.Run("RecordingSpan", func(t *testing.T) {
			// The first time we have a recording span, we
			// should use protoreflect to create extractors
			// for each of the attributes. This should cause
			// any configuration errors to be reported.
			span := mock.NewMockSpan(ctrl)
			ctxWithSpan := trace.ContextWithSpan(ctx, span)
			span.EXPECT().IsRecording().Return(true).AnyTimes()
			errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to create extractor for attribute \"request\": Attribute name does not contain any fields")))
			errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to create extractor for attribute \"response.cache_capabilities\": Field \"cache_capabilities\" does not have a boolean, enumeration, floating point, integer, bytes or string type")))
			errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to create extractor for attribute \"response.nonexistent\": Field \"nonexistent\" does not exist")))
			errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to create extractor for attribute \"response.execution_capabilities.execution_priority_capabilities.priorities.min_priority\": Field \"priorities\" does not refer to a singular message")))
			span.EXPECT().SetAttributes([]attribute.KeyValue{
				attribute.String("request.instance_name", "default-scheduler"),
			})
			var observedResponse remoteexecution.ServerCapabilities
			invoker.EXPECT().Call(ctxWithSpan, exampleUnaryFullMethod, exampleUnaryRequest, &observedResponse, nil).
				Do(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					proto.Merge(reply.(proto.Message), exampleUnaryResponse)
					return nil
				})
			span.EXPECT().SetAttributes([]attribute.KeyValue{
				attribute.StringSlice("response.cache_capabilities.digest_functions", []string{"MD5", "SHA256"}),
				attribute.Int64("response.cache_capabilities.max_batch_total_size_bytes", 1<<20),
				attribute.Bool("response.execution_capabilities.exec_enabled", true),
				attribute.StringSlice("response.execution_capabilities.supported_node_properties", []string{"CTime", "MTime"}),
			})

			require.NoError(t, extractor.InterceptUnaryClient(ctxWithSpan, exampleUnaryFullMethod, exampleUnaryRequest, &observedResponse, nil, invoker.Call))
			testutil.RequireEqualProto(t, exampleUnaryResponse, &observedResponse)
		})
	})

	t.Run("InterceptUnaryServer", func(t *testing.T) {
		handler := mock.NewMockUnaryHandler(ctrl)

		t.Run("NoSpanInContext", func(t *testing.T) {
			// If there is no tracing span in the context,
			// the call should be forwarded in literal form.
			handler.EXPECT().Call(ctx, exampleUnaryRequest).Return(exampleUnaryResponse, nil)

			observedResponse, err := extractor.InterceptUnaryServer(ctx, exampleUnaryRequest, &grpc.UnaryServerInfo{
				FullMethod: exampleUnaryFullMethod,
			}, handler.Call)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, exampleUnaryResponse, observedResponse.(proto.Message))
		})

		t.Run("UnknownMethod", func(t *testing.T) {
			// Methods for which we've not specified a
			// configuration should have their calls
			// forwarded in literal form.
			span := mock.NewMockSpan(ctrl)
			ctxWithSpan := trace.ContextWithSpan(ctx, span)
			request := &remoteexecution.UpdateActionResultRequest{
				InstanceName: "default-scheduler",
			}
			response := &remoteexecution.ActionResult{}
			handler.EXPECT().Call(ctxWithSpan, request).Return(response, nil)

			observedResponse, err := extractor.InterceptUnaryServer(ctxWithSpan, request, &grpc.UnaryServerInfo{
				FullMethod: "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult",
			}, handler.Call)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, response, observedResponse.(proto.Message))
		})

		t.Run("NonRecordingSpan", func(t *testing.T) {
			// If the span is not recording, we shouldn't
			// spend any effort inspecting the request and
			// response.
			span := mock.NewMockSpan(ctrl)
			ctxWithSpan := trace.ContextWithSpan(ctx, span)
			span.EXPECT().IsRecording().Return(false)
			handler.EXPECT().Call(ctxWithSpan, exampleUnaryRequest).Return(exampleUnaryResponse, nil)

			observedResponse, err := extractor.InterceptUnaryServer(ctxWithSpan, exampleUnaryRequest, &grpc.UnaryServerInfo{
				FullMethod: exampleUnaryFullMethod,
			}, handler.Call)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, exampleUnaryResponse, observedResponse.(proto.Message))
		})

		t.Run("RecordingSpan", func(t *testing.T) {
			// As the span is recording, we should see calls
			// to SetAttributes().
			span := mock.NewMockSpan(ctrl)
			ctxWithSpan := trace.ContextWithSpan(ctx, span)
			span.EXPECT().IsRecording().Return(true).AnyTimes()
			span.EXPECT().SetAttributes([]attribute.KeyValue{
				attribute.String("request.instance_name", "default-scheduler"),
			})
			handler.EXPECT().Call(ctxWithSpan, exampleUnaryRequest).Return(exampleUnaryResponse, nil)
			span.EXPECT().SetAttributes([]attribute.KeyValue{
				attribute.StringSlice("response.cache_capabilities.digest_functions", []string{"MD5", "SHA256"}),
				attribute.Int64("response.cache_capabilities.max_batch_total_size_bytes", 1<<20),
				attribute.Bool("response.execution_capabilities.exec_enabled", true),
				attribute.StringSlice("response.execution_capabilities.supported_node_properties", []string{"CTime", "MTime"}),
			})

			observedResponse, err := extractor.InterceptUnaryServer(ctxWithSpan, exampleUnaryRequest, &grpc.UnaryServerInfo{
				FullMethod: exampleUnaryFullMethod,
			}, handler.Call)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, exampleUnaryResponse, observedResponse.(proto.Message))
		})
	})
}

func TestProtoTraceAttributesExtractorStreaming(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	t.Run("InterceptStreamClient", func(t *testing.T) {
		span := mock.NewMockSpan(ctrl)
		ctxWithSpan := trace.ContextWithSpan(ctx, span)
		streamMethod := "/build.bazel.remote.execution.v2.Capabilities/StreamCapabilities"
		streamDesc := grpc.StreamDesc{StreamName: "StreamCapabilities", ClientStreams: true, ServerStreams: true}
		streamer := mock.NewMockStreamer(ctrl)
		clientStream := mock.NewMockClientStream(ctrl)

		extractor := bb_grpc.NewProtoTraceAttributesExtractor(map[string]*configuration.TracingMethodConfiguration{
			streamMethod: {
				AttributesFromFirstRequestMessage: []string{
					"instance_name",
				},
				AttributesFromFirstResponseMessage: []string{
					"execution_capabilities.exec_enabled",
				},
			},
		}, mock.NewMockErrorLogger(ctrl))

		request := &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "default-scheduler",
		}
		response := &remoteexecution.ServerCapabilities{
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}

		span.EXPECT().IsRecording().Return(true).AnyTimes()
		streamer.EXPECT().Call(ctxWithSpan, &streamDesc, nil, streamMethod).Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(request).Return(nil)
		clientStream.EXPECT().SendMsg(request).Return(nil)
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(
			func(m interface{}) error {
				proto.Merge(m.(proto.Message), response)
				return nil
			}).Times(2)

		span.EXPECT().SetAttributes([]attribute.KeyValue{
			attribute.String("request.instance_name", "default-scheduler"),
		})
		span.EXPECT().SetAttributes([]attribute.KeyValue{
			attribute.Bool("response.execution_capabilities.exec_enabled", true),
		})

		var events []trace.EventConfig
		span.EXPECT().AddEvent("grpc.message", gomock.Any()).AnyTimes().Do(
			func(_ string, options ...trace.EventOption) {
				events = append(events, trace.NewEventConfig(options...))
			})

		wrappedStream, err := extractor.InterceptStreamClient(ctxWithSpan, &streamDesc, nil, streamMethod, streamer.Call)
		require.NoError(t, err)

		require.NoError(t, wrappedStream.SendMsg(request))
		require.NoError(t, wrappedStream.SendMsg(request))
		var observedResponse remoteexecution.ServerCapabilities
		require.NoError(t, wrappedStream.RecvMsg(&observedResponse))
		require.NoError(t, wrappedStream.RecvMsg(&observedResponse))

		require.Len(t, events, 4)
		attributes := findEventAttributes(t, events, "out", 1)
		require.True(t, hasAttribute(attributes, attribute.String("request.instance_name", "default-scheduler")))
		attributes = findEventAttributes(t, events, "out", 2)
		require.True(t, hasAttribute(attributes, attribute.String("request.instance_name", "default-scheduler")))
		attributes = findEventAttributes(t, events, "in", 1)
		require.True(t, hasAttribute(attributes, attribute.Bool("response.execution_capabilities.exec_enabled", true)))
		attributes = findEventAttributes(t, events, "in", 2)
		require.True(t, hasAttribute(attributes, attribute.Bool("response.execution_capabilities.exec_enabled", true)))
	})

	t.Run("InterceptStreamServer", func(t *testing.T) {
		span := mock.NewMockSpan(ctrl)
		ctxWithSpan := trace.ContextWithSpan(ctx, span)
		streamMethod := "/build.bazel.remote.execution.v2.Capabilities/StreamCapabilities"
		serverStream := mock.NewMockServerStream(ctrl)
		handler := mock.NewMockStreamHandler(ctrl)

		extractor := bb_grpc.NewProtoTraceAttributesExtractor(map[string]*configuration.TracingMethodConfiguration{
			streamMethod: {
				AttributesFromFirstRequestMessage: []string{
					"instance_name",
				},
				AttributesFromFirstResponseMessage: []string{
					"execution_capabilities.exec_enabled",
				},
			},
		}, mock.NewMockErrorLogger(ctrl))

		requestOne := &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "first",
		}
		requestTwo := &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "second",
		}
		responseOne := &remoteexecution.ServerCapabilities{
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}
		responseTwo := &remoteexecution.ServerCapabilities{
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: false,
			},
		}

		span.EXPECT().IsRecording().Return(true).AnyTimes()
		serverStream.EXPECT().Context().Return(ctxWithSpan).AnyTimes()
		serverStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(
			func(m interface{}) error {
				proto.Merge(m.(proto.Message), requestOne)
				return nil
			})
		serverStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(
			func(m interface{}) error {
				proto.Merge(m.(proto.Message), requestTwo)
				return nil
			})
		serverStream.EXPECT().SendMsg(responseOne).Return(nil)
		serverStream.EXPECT().SendMsg(responseTwo).Return(nil)

		span.EXPECT().SetAttributes([]attribute.KeyValue{
			attribute.String("request.instance_name", "first"),
		})
		span.EXPECT().SetAttributes([]attribute.KeyValue{
			attribute.Bool("response.execution_capabilities.exec_enabled", true),
		})

		var events []trace.EventConfig
		span.EXPECT().AddEvent("grpc.message", gomock.Any()).AnyTimes().Do(
			func(_ string, options ...trace.EventOption) {
				events = append(events, trace.NewEventConfig(options...))
			})

		handler.EXPECT().Call(nil, gomock.Any()).DoAndReturn(
			func(srv interface{}, stream grpc.ServerStream) error {
				var observedRequest remoteexecution.GetCapabilitiesRequest
				require.NoError(t, stream.RecvMsg(&observedRequest))
				require.NoError(t, stream.RecvMsg(&observedRequest))
				require.NoError(t, stream.SendMsg(responseOne))
				require.NoError(t, stream.SendMsg(responseTwo))
				return nil
			})

		require.NoError(t, extractor.InterceptStreamServer(nil, serverStream, &grpc.StreamServerInfo{
			FullMethod: streamMethod,
		}, handler.Call))

		require.Len(t, events, 4)
		attributes := findEventAttributes(t, events, "in", 1)
		require.True(t, hasAttribute(attributes, attribute.String("request.instance_name", "first")))
		attributes = findEventAttributes(t, events, "in", 2)
		require.True(t, hasAttribute(attributes, attribute.String("request.instance_name", "second")))
		attributes = findEventAttributes(t, events, "out", 1)
		require.True(t, hasAttribute(attributes, attribute.Bool("response.execution_capabilities.exec_enabled", true)))
		attributes = findEventAttributes(t, events, "out", 2)
		require.True(t, hasAttribute(attributes, attribute.Bool("response.execution_capabilities.exec_enabled", false)))
	})
}
