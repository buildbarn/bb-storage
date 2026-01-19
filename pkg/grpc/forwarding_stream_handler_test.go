package grpc_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"go.uber.org/mock/gomock"
)

type eqProtoStringValueMatcher struct {
	gomock.Matcher
}

// newEqProtoStringValueMatcher is a gomock matcher for proto equality after
// converting the proto.Message to structpb.Value.
func newEqProtoStringValueMatcher(t *testing.T, v string) gomock.Matcher {
	proto := structpb.NewStringValue(v)
	return &eqProtoStringValueMatcher{
		Matcher: testutil.EqProto(t, proto),
	}
}

func (m *eqProtoStringValueMatcher) Matches(other interface{}) bool {
	otherProto, ok := other.(proto.Message)
	if !ok {
		return false
	}
	bytes, err := proto.Marshal(otherProto)
	if err != nil {
		return false
	}
	value := new(structpb.Value)
	if proto.Unmarshal(bytes, value) != nil {
		return false
	}
	return m.Matcher.Matches(value)
}

func newForwardingStreamRecvMsgStub(v string) func(msg any) error {
	src := structpb.NewStringValue(v)
	bytes, err := proto.Marshal(src)
	return func(dst any) error {
		if err != nil {
			return err
		}
		return proto.Unmarshal(bytes, dst.(proto.Message))
	}
}

func TestSimpleStreamForwarder(t *testing.T) {
	ctrl, _ := gomock.WithContext(context.Background(), t)

	backend := mock.NewMockClientConnInterface(ctrl)
	forwarder := bb_grpc.NewForwardingStreamHandler(backend)
	serverTransportStream := mock.NewMockServerTransportStream(ctrl)
	serverTransportStream.EXPECT().Method().Return("/serviceA/method1").AnyTimes()

	t.Run("RequestSuccess", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var outgoingStreamCtx context.Context
			outgoingRecvBarrier := make(chan struct{})
			incomingStreamCtx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
			incomingStream := mock.NewMockServerStream(ctrl)
			outgoingStream := mock.NewMockClientStream(ctrl)

			newStreamCall := backend.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/serviceA/method1").DoAndReturn(
				func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					outgoingStreamCtx = ctx
					return outgoingStream, nil
				},
			)

			incomingStream.EXPECT().Context().Return(incomingStreamCtx).AnyTimes()
			gomock.InOrder(
				newStreamCall,
				incomingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(newForwardingStreamRecvMsgStub("beep")),
				outgoingStream.EXPECT().SendMsg(newEqProtoStringValueMatcher(t, "beep")).Return(nil),
				incomingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(newForwardingStreamRecvMsgStub("boop")),
				outgoingStream.EXPECT().SendMsg(newEqProtoStringValueMatcher(t, "boop")).Return(nil),
				incomingStream.EXPECT().RecvMsg(gomock.Any()).Return(io.EOF),
				outgoingStream.EXPECT().CloseSend().DoAndReturn(func() error {
					close(outgoingRecvBarrier)
					return nil
				}),
			)
			gomock.InOrder(
				newStreamCall,
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
					<-outgoingRecvBarrier
					synctest.Wait()
					require.NoError(t, outgoingStreamCtx.Err())
					return io.EOF
				}),
			)

			require.NoError(t, forwarder(nil, incomingStream))
			<-outgoingStreamCtx.Done()
		})
	})

	t.Run("RequestRecvError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var outgoingStreamCtx context.Context
			incomingStreamCtx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
			incomingStream := mock.NewMockServerStream(ctrl)
			outgoingStream := mock.NewMockClientStream(ctrl)

			newStreamCall := backend.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/serviceA/method1").DoAndReturn(
				func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					outgoingStreamCtx = ctx
					return outgoingStream, nil
				},
			)

			incomingStream.EXPECT().Context().Return(incomingStreamCtx).AnyTimes()
			gomock.InOrder(
				newStreamCall,
				incomingStream.EXPECT().RecvMsg(gomock.Any()).Return(errors.New("incoming recv")),
			)
			gomock.InOrder(
				newStreamCall,
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
					// When incomingStream.RecvMsg returns, the backend context
					// should be canceled due to the error.
					<-outgoingStreamCtx.Done()
					return context.Canceled
				}),
			)

			require.EqualError(t, forwarder(nil, incomingStream), "incoming recv")
		})
	})

	t.Run("RequestSendError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var outgoingStreamCtx context.Context
			incomingStreamCtx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
			incomingStream := mock.NewMockServerStream(ctrl)
			outgoingStream := mock.NewMockClientStream(ctrl)

			newStreamCall := backend.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/serviceA/method1").DoAndReturn(
				func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					outgoingStreamCtx = ctx
					return outgoingStream, nil
				},
			)

			incomingStream.EXPECT().Context().Return(incomingStreamCtx).AnyTimes()
			gomock.InOrder(
				newStreamCall,
				incomingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(newForwardingStreamRecvMsgStub("beep")),
				outgoingStream.EXPECT().SendMsg(newEqProtoStringValueMatcher(t, "beep")).Return(errors.New("outgoing send")),
			)
			gomock.InOrder(
				newStreamCall,
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
					// When outgoingStream.SendMsg returns, the outgoing context
					// should be canceled due to the error.
					<-outgoingStreamCtx.Done()
					return context.Canceled
				}),
			)

			require.EqualError(t, forwarder(nil, incomingStream), "outgoing send")
		})
	})

	t.Run("ResponseSuccess", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var outgoingStreamCtx context.Context
			incomingRecvBarrier := make(chan struct{})
			incomingStreamCtx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
			incomingStream := mock.NewMockServerStream(ctrl)
			outgoingStream := mock.NewMockClientStream(ctrl)

			newStreamCall := backend.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/serviceA/method1").DoAndReturn(
				func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					outgoingStreamCtx = ctx
					return outgoingStream, nil
				},
			)

			incomingStream.EXPECT().Context().Return(incomingStreamCtx).AnyTimes()
			gomock.InOrder(
				newStreamCall,
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(newForwardingStreamRecvMsgStub("beep")),
				incomingStream.EXPECT().SendMsg(newEqProtoStringValueMatcher(t, "beep")).Return(nil),
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(newForwardingStreamRecvMsgStub("boop")),
				incomingStream.EXPECT().SendMsg(newEqProtoStringValueMatcher(t, "boop")).Return(nil),
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).Return(io.EOF),
			)
			gomock.InOrder(
				newStreamCall,
				incomingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
					<-incomingRecvBarrier
					return context.Canceled
				}),
			)

			require.NoError(t, forwarder(nil, incomingStream))
			<-outgoingStreamCtx.Done()

			// incomingStream.Recv() is still blocking.
			close(incomingRecvBarrier)
		})
	})

	t.Run("ResponseRecvError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var outgoingStreamCtx context.Context
			incomingRecvBarrier := make(chan struct{})
			incomingStreamCtx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
			incomingStream := mock.NewMockServerStream(ctrl)
			outgoingStream := mock.NewMockClientStream(ctrl)

			newStreamCall := backend.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/serviceA/method1").DoAndReturn(
				func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					outgoingStreamCtx = ctx
					return outgoingStream, nil
				},
			)

			incomingStream.EXPECT().Context().Return(incomingStreamCtx).AnyTimes()
			gomock.InOrder(
				newStreamCall,
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).Return(errors.New("outgoing recv")),
			)
			gomock.InOrder(
				newStreamCall,
				incomingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
					<-incomingRecvBarrier
					return context.Canceled
				}),
			)

			require.EqualError(t, forwarder(nil, incomingStream), "outgoing recv")
			<-outgoingStreamCtx.Done()

			// incomingStream.Recv() is still blocking.
			close(incomingRecvBarrier)
		})
	})

	t.Run("ResponseSendError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var outgoingStreamCtx context.Context
			incomingStreamCtx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
			incomingStream := mock.NewMockServerStream(ctrl)
			outgoingStream := mock.NewMockClientStream(ctrl)

			newStreamCall := backend.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/serviceA/method1").DoAndReturn(
				func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					outgoingStreamCtx = ctx
					return outgoingStream, nil
				},
			)

			incomingStream.EXPECT().Context().Return(incomingStreamCtx).AnyTimes()
			gomock.InOrder(
				newStreamCall,
				outgoingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(newForwardingStreamRecvMsgStub("beep")),
				incomingStream.EXPECT().SendMsg(newEqProtoStringValueMatcher(t, "beep")).Return(errors.New("incoming send")),
			)
			gomock.InOrder(
				newStreamCall,
				incomingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
					// When incomingStream.SendMsg returns, the outgoing context
					// should be canceled due to the error.
					<-outgoingStreamCtx.Done()
					return context.Canceled
				}),
			)

			require.EqualError(t, forwarder(nil, incomingStream), "incoming send")
		})
	})

	t.Run("NewStreamError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			incomingStreamCtx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
			incomingStream := mock.NewMockServerStream(ctrl)

			incomingStream.EXPECT().Context().Return(incomingStreamCtx).AnyTimes()
			backend.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/serviceA/method1").Return(nil, errors.New("no stream"))

			require.EqualError(t, forwarder(nil, incomingStream), "no stream")
		})
	})
}
