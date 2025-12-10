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

// simpleStreamForwarderStandardFixture contains channels to communicate with
// the RecvMsg and SendMsg mocks in the incoming and backend streams.
type simpleStreamForwarderStandardFixture struct {
	forwarder      grpc.StreamHandler
	incomingStream *mock.MockServerStream

	backendNewStreamErrorChan chan<- error
	backendNewStreamCtx       <-chan context.Context

	// IncomingRecvErrorChan provides the return value for
	// incomingStream.RecvMsg().
	IncomingRecvErrorChan chan<- error
	// IncomingRecvValueChan provides the returned proto message for
	// incomingStream.RecvMsg() if the error was nil.
	IncomingRecvValueChan chan<- *structpb.Value
	// BackendSendErrorChan provides the return value for
	// backendStream.SendMsg().
	BackendSendErrorChan chan<- error
	// BackendSendValueChan receives the proto message provided in the call to
	// backendStream.SendMsg().
	BackendSendValueChan <-chan *structpb.Value
	// BackendSendCloseChan receives an entry when backendStream.CloseSend() is
	// called.
	BackendSendCloseChan <-chan struct{}
	// BackendRecvErrorChan provides the return value for
	// backendStream.RecvMsg().
	BackendRecvErrorChan chan<- error
	// BackendRecvValueChan provides the returned proto message for
	// backendStream.RecvMsg() if the error was nil.
	BackendRecvValueChan chan<- *structpb.Value
	// IncomingSendErrorChan provides the return value for
	// incomingStream.SendMsg().
	IncomingSendErrorChan chan<- error
	// IncomingSendValueChan receives the proto message provided in the call to
	// incomingStream.SendMsg().
	IncomingSendValueChan <-chan *structpb.Value
}

func newSimpleStreamForwarderStandardFixture(ctx context.Context, ctrl *gomock.Controller, t *testing.T) *simpleStreamForwarderStandardFixture {
	backend := mock.NewMockClientConnInterface(ctrl)
	forwarder := bb_grpc.NewForwardingStreamHandler(backend)
	serverTransportStream := mock.NewMockServerTransportStream(ctrl)
	serverTransportStream.EXPECT().Method().Return("/buildbarn.buildqueuestate.BuildQueueState/ListWorkers").AnyTimes()
	streamCtx := grpc.NewContextWithServerTransportStream(ctx, serverTransportStream)
	incomingStream := mock.NewMockServerStream(ctrl)
	incomingStream.EXPECT().Context().Return(streamCtx).AnyTimes()
	backendStream := mock.NewMockClientStream(ctrl)

	backendNewStreamErrorChan := make(chan error, 10)
	backendNewStreamCtx := make(chan context.Context, 10)

	backend.EXPECT().NewStream(
		gomock.Any(),
		gomock.Any(),
		"/buildbarn.buildqueuestate.BuildQueueState/ListWorkers",
	).DoAndReturn(func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		backendNewStreamCtx <- ctx
		return backendStream, <-backendNewStreamErrorChan
	}).AnyTimes()

	incomingRecvErrorChan := make(chan error, 10)
	incomingRecvValueChan := make(chan *structpb.Value, 10)
	backendSendErrorChan := make(chan error, 10)
	backendSendValueChan := make(chan *structpb.Value, 10)
	backendSendCloseChan := make(chan struct{}, 10)
	backendRecvErrorChan := make(chan error, 10)
	backendRecvValueChan := make(chan *structpb.Value, 10)
	incomingSendErrorChan := make(chan error, 10)
	incomingSendValueChan := make(chan *structpb.Value, 10)

	incomingStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
		if err := <-incomingRecvErrorChan; err != nil {
			return err
		}
		value := <-incomingRecvValueChan
		bytes, err := proto.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, proto.Unmarshal(bytes, msg.(proto.Message)))
		return nil
	}).AnyTimes()
	backendStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg any) error {
		if err := <-backendSendErrorChan; err != nil {
			return err
		}
		bytes, err := proto.Marshal(msg.(proto.Message))
		require.NoError(t, err)
		value := new(structpb.Value)
		require.NoError(t, proto.Unmarshal(bytes, value))
		backendSendValueChan <- value
		return nil
	}).AnyTimes()
	backendStream.EXPECT().CloseSend().DoAndReturn(func() error {
		backendSendCloseChan <- struct{}{}
		return nil
	}).AnyTimes()
	backendStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(msg any) error {
		if err := <-backendRecvErrorChan; err != nil {
			return err
		}
		value := <-backendRecvValueChan
		bytes, err := proto.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, proto.Unmarshal(bytes, msg.(proto.Message)))
		return nil
	}).AnyTimes()
	incomingStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg any) error {
		if err := <-incomingSendErrorChan; err != nil {
			return err
		}
		bytes, err := proto.Marshal(msg.(proto.Message))
		require.NoError(t, err)
		value := new(structpb.Value)
		require.NoError(t, proto.Unmarshal(bytes, value))
		incomingSendValueChan <- value
		return nil
	}).AnyTimes()

	return &simpleStreamForwarderStandardFixture{
		forwarder:      forwarder,
		incomingStream: incomingStream,

		backendNewStreamErrorChan: backendNewStreamErrorChan,
		backendNewStreamCtx:       backendNewStreamCtx,

		IncomingRecvErrorChan: incomingRecvErrorChan,
		IncomingRecvValueChan: incomingRecvValueChan,
		BackendSendErrorChan:  backendSendErrorChan,
		BackendSendValueChan:  backendSendValueChan,
		BackendSendCloseChan:  backendSendCloseChan,
		BackendRecvErrorChan:  backendRecvErrorChan,
		BackendRecvValueChan:  backendRecvValueChan,
		IncomingSendErrorChan: incomingSendErrorChan,
		IncomingSendValueChan: incomingSendValueChan,
	}
}

func (f *simpleStreamForwarderStandardFixture) call(newStreamErr error) (context.Context, <-chan error) {
	callResult := make(chan error, 1)
	go func() {
		defer close(callResult)
		f.backendNewStreamErrorChan <- newStreamErr
		callResult <- f.forwarder(nil, f.incomingStream)
	}()
	return <-f.backendNewStreamCtx, callResult
}

func (f *simpleStreamForwarderStandardFixture) verifyEmptyChannels(t *testing.T) {
	require.Len(t, f.backendNewStreamErrorChan, 0, "backendNewStreamErrorChan")
	require.Len(t, f.backendNewStreamCtx, 0, "backendNewStreamCtx")
	require.Len(t, f.IncomingRecvErrorChan, 0, "IncomingRecvErrorChan")
	require.Len(t, f.IncomingRecvValueChan, 0, "IncomingRecvValueChan")
	require.Len(t, f.BackendSendErrorChan, 0, "BackendSendErrorChan")
	require.Len(t, f.BackendSendValueChan, 0, "BackendSendValueChan")
	require.Len(t, f.BackendSendCloseChan, 0, "BackendSendCloseChan")
	require.Len(t, f.BackendRecvErrorChan, 0, "BackendRecvErrorChan")
	require.Len(t, f.BackendRecvValueChan, 0, "BackendRecvValueChan")
	require.Len(t, f.IncomingSendErrorChan, 0, "IncomingSendErrorChan")
	require.Len(t, f.IncomingSendValueChan, 0, "IncomingSendValueChan")
}

func TestSimpleStreamForwarderRequestSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)
		fixture := newSimpleStreamForwarderStandardFixture(ctx, ctrl, t)
		backendCtx, forwardResultChan := fixture.call(nil)

		fixture.IncomingRecvErrorChan <- nil
		fixture.IncomingRecvValueChan <- structpb.NewStringValue("beep")
		fixture.BackendSendErrorChan <- nil
		testutil.RequireEqualProto(t, structpb.NewStringValue("beep"), <-fixture.BackendSendValueChan)
		fixture.IncomingRecvErrorChan <- nil
		fixture.IncomingRecvValueChan <- structpb.NewStringValue("boop")
		fixture.BackendSendErrorChan <- nil
		testutil.RequireEqualProto(t, structpb.NewStringValue("boop"), <-fixture.BackendSendValueChan)

		// Should still be forwarding requests to the backend.
		synctest.Wait()
		require.Len(t, fixture.BackendSendCloseChan, 0)
		fixture.IncomingRecvErrorChan <- io.EOF
		<-fixture.BackendSendCloseChan

		// Should still be receiving responses.
		synctest.Wait()
		testutil.VerifyChannelIsBlocking(t, backendCtx.Done())
		require.Len(t, forwardResultChan, 0)
		fixture.BackendRecvErrorChan <- io.EOF
		<-backendCtx.Done()
		require.NoError(t, <-forwardResultChan)

		fixture.verifyEmptyChannels(t)
	})
}

func TestSimpleStreamForwarderRequestRecvError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)
		fixture := newSimpleStreamForwarderStandardFixture(ctx, ctrl, t)
		backendCtx, forwardResultChan := fixture.call(nil)

		fixture.IncomingRecvErrorChan <- errors.New("incoming recv")

		// In error state, so the backend context should be canceled.
		<-backendCtx.Done()
		// Emulate that backend.RecvMsg() to returns.
		fixture.BackendRecvErrorChan <- context.Canceled

		require.EqualError(t, <-forwardResultChan, "incoming recv")

		fixture.verifyEmptyChannels(t)
	})
}

func TestSimpleStreamForwarderRequestSendError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)
		fixture := newSimpleStreamForwarderStandardFixture(ctx, ctrl, t)
		backendCtx, forwardResultChan := fixture.call(nil)

		fixture.IncomingRecvErrorChan <- nil
		fixture.IncomingRecvValueChan <- structpb.NewStringValue("beep")
		fixture.BackendSendErrorChan <- errors.New("backend send")

		// In error state, so the backend context should be canceled.
		<-backendCtx.Done()
		// Emulate that backend.RecvMsg() to returns.
		fixture.BackendRecvErrorChan <- context.Canceled

		require.EqualError(t, <-forwardResultChan, "backend send")

		fixture.verifyEmptyChannels(t)
	})
}

func TestSimpleStreamForwarderResponseSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)
		fixture := newSimpleStreamForwarderStandardFixture(ctx, ctrl, t)
		backendCtx, forwardResultChan := fixture.call(nil)

		fixture.BackendRecvErrorChan <- nil
		fixture.BackendRecvValueChan <- structpb.NewStringValue("beep")
		fixture.IncomingSendErrorChan <- nil
		testutil.RequireEqualProto(t, structpb.NewStringValue("beep"), <-fixture.IncomingSendValueChan)
		fixture.BackendRecvErrorChan <- nil
		fixture.BackendRecvValueChan <- structpb.NewStringValue("boop")
		fixture.IncomingSendErrorChan <- nil
		testutil.RequireEqualProto(t, structpb.NewStringValue("boop"), <-fixture.IncomingSendValueChan)

		// Should still be forwarding requests to the backend.
		synctest.Wait()
		require.Len(t, fixture.BackendSendCloseChan, 0)
		// Should still be receiving responses.
		testutil.VerifyChannelIsBlocking(t, backendCtx.Done())
		require.Len(t, forwardResultChan, 0)
		fixture.BackendRecvErrorChan <- io.EOF
		// Will not receive any more from the backend, so its context should
		// be canceled.
		<-backendCtx.Done()
		require.NoError(t, <-forwardResultChan)
		fixture.IncomingRecvErrorChan <- context.Canceled

		fixture.verifyEmptyChannels(t)
	})
}

func TestSimpleStreamForwarderResponseRecvError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)
		fixture := newSimpleStreamForwarderStandardFixture(ctx, ctrl, t)
		backendCtx, forwardResultChan := fixture.call(nil)

		fixture.BackendRecvErrorChan <- errors.New("backend recv")

		// In error state, so the backend context should be canceled.
		<-backendCtx.Done()
		require.EqualError(t, <-forwardResultChan, "backend recv")

		// Emulate that incoming.RecvMsg() returns now when the whole stream
		// handler has returned.
		fixture.IncomingRecvErrorChan <- context.Canceled

		fixture.verifyEmptyChannels(t)
	})
}

func TestSimpleStreamForwarderResponseSendError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)
		fixture := newSimpleStreamForwarderStandardFixture(ctx, ctrl, t)
		backendCtx, forwardResultChan := fixture.call(nil)

		fixture.BackendRecvErrorChan <- nil
		fixture.BackendRecvValueChan <- structpb.NewStringValue("beep")
		fixture.IncomingSendErrorChan <- errors.New("incoming send")

		// In error state, so the backend context should be canceled.
		<-backendCtx.Done()

		// The forwarder should return, even if the backend.RecvMsg() is slow.
		require.EqualError(t, <-forwardResultChan, "incoming send")

		// Emulate that incoming.RecvMsg() returns now when the whole stream
		// handler has returned.
		fixture.IncomingRecvErrorChan <- context.Canceled

		fixture.verifyEmptyChannels(t)
	})
}
