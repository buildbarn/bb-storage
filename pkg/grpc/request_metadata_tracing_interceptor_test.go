package grpc_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.uber.org/mock/gomock"
)

func TestRequestMetadataTracingUnaryInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Example RequestMetadata.
	requestMetadataBin, err := proto.Marshal(&remoteexecution.RequestMetadata{
		ToolDetails: &remoteexecution.ToolDetails{
			ToolName:    "bazel",
			ToolVersion: "4.1.0",
		},
		ActionId:                "b61aa70754d2b7cc2e020f5a1826f542eba3bef83c788e1e550e04dc11c0e88c",
		ToolInvocationId:        "d4e49e67-2bae-4a41-aaf1-8faa379254ce",
		CorrelatedInvocationsId: "5b1b30d4-97d4-432c-9902-448fbc95d86e",
		ActionMnemonic:          "CppLink",
		TargetId:                "//hello_world",
		ConfigurationId:         "c03b730e3ebb0c7fe1d7f8d7046acc97bc3fbc498bcd4866aec7aa09729072b9",
	})
	require.NoError(t, err)
	ctxWithRequestMetadata := metadata.NewIncomingContext(
		ctx,
		metadata.Pairs("build.bazel.remote.execution.v2.requestmetadata-bin", string(requestMetadataBin)))

	t.Run("NoSpan", func(t *testing.T) {
		// In case the provided Context does not provide a trace
		// span, the interceptor should effectively do nothing.
		unaryHandler := mock.NewMockUnaryHandler(ctrl)
		unaryHandler.EXPECT().Call(gomock.Any(), &emptypb.Empty{}).Return(&emptypb.Empty{}, nil)

		_, err := bb_grpc.RequestMetadataTracingUnaryInterceptor(
			ctxWithRequestMetadata,
			&emptypb.Empty{},
			&grpc.UnaryServerInfo{},
			unaryHandler.Call)
		require.NoError(t, err)
	})

	t.Run("NonRecordingSpan", func(t *testing.T) {
		// If a span is provided that is not doing any
		// recording, then there is no point in unmarshaling the
		// RequestMetadata header.
		span := mock.NewMockSpan(ctrl)
		span.EXPECT().IsRecording().Return(false)
		unaryHandler := mock.NewMockUnaryHandler(ctrl)
		unaryHandler.EXPECT().Call(gomock.Any(), &emptypb.Empty{}).Return(&emptypb.Empty{}, nil)

		_, err := bb_grpc.RequestMetadataTracingUnaryInterceptor(
			trace.ContextWithSpan(ctxWithRequestMetadata, span),
			&emptypb.Empty{},
			&grpc.UnaryServerInfo{},
			unaryHandler.Call)
		require.NoError(t, err)
	})

	t.Run("NoMetadata", func(t *testing.T) {
		// If the client provided no RequestMetadata message, no
		// spans should be added.
		span := mock.NewMockSpan(ctrl)
		span.EXPECT().IsRecording().Return(true)
		unaryHandler := mock.NewMockUnaryHandler(ctrl)
		unaryHandler.EXPECT().Call(gomock.Any(), &emptypb.Empty{}).Return(&emptypb.Empty{}, nil)

		_, err := bb_grpc.RequestMetadataTracingUnaryInterceptor(
			trace.ContextWithSpan(ctx, span),
			&emptypb.Empty{},
			&grpc.UnaryServerInfo{},
			unaryHandler.Call)
		require.NoError(t, err)
	})

	t.Run("Success", func(t *testing.T) {
		// If the current span is recording and a valid
		// RequestMetadata message is part of the gRPC incoming
		// metadata, all fields in the RequestMetadata should be
		// added as attributes to the span.
		span := mock.NewMockSpan(ctrl)
		span.EXPECT().IsRecording().Return(true)
		span.EXPECT().SetAttributes(gomock.InAnyOrder([]attribute.KeyValue{
			attribute.String("request_metadata.tool_details.tool_name", "bazel"),
			attribute.String("request_metadata.tool_details.tool_version", "4.1.0"),
			attribute.String("request_metadata.action_id", "b61aa70754d2b7cc2e020f5a1826f542eba3bef83c788e1e550e04dc11c0e88c"),
			attribute.String("request_metadata.tool_invocation_id", "d4e49e67-2bae-4a41-aaf1-8faa379254ce"),
			attribute.String("request_metadata.correlated_invocations_id", "5b1b30d4-97d4-432c-9902-448fbc95d86e"),
			attribute.String("request_metadata.action_mnemonic", "CppLink"),
			attribute.String("request_metadata.target_id", "//hello_world"),
			attribute.String("request_metadata.configuration_id", "c03b730e3ebb0c7fe1d7f8d7046acc97bc3fbc498bcd4866aec7aa09729072b9"),
		}))
		unaryHandler := mock.NewMockUnaryHandler(ctrl)
		unaryHandler.EXPECT().Call(gomock.Any(), &emptypb.Empty{}).Return(&emptypb.Empty{}, nil)

		_, err := bb_grpc.RequestMetadataTracingUnaryInterceptor(
			trace.ContextWithSpan(ctxWithRequestMetadata, span),
			&emptypb.Empty{},
			&grpc.UnaryServerInfo{},
			unaryHandler.Call)
		require.NoError(t, err)
	})
}
