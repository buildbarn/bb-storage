package grpc_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestProtoTraceAttributesExtractor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Common definitions used by the tests below.
	span := mock.NewMockSpan(ctrl)
	ctxWithSpan := trace.ContextWithSpan(ctx, span)
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
			span.EXPECT().IsRecording().Return(true)
			errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to create extractor for attribute \"request\": Attribute name does not contain any fields")))
			errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to create extractor for attribute \"response.cache_capabilities\": Field \"cache_capabilities\" does not have a boolean, enumeration, floating point, signed integer or string type")))
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
			span.EXPECT().IsRecording().Return(true)
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

	// TODO: Add testing coverage for streaming RPCs.
}
