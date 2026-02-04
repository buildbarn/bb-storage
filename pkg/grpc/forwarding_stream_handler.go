package grpc

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NewForwardingStreamHandler creates a grpc.StreamHandler that forwards gRPC
// calls to a grpc.ClientConnInterface backend.
func NewForwardingStreamHandler(client grpc.ClientConnInterface) grpc.StreamHandler {
	forwarder := &forwardingStreamHandler{
		backend: client,
	}
	return forwarder.HandleStream
}

type forwardingStreamHandler struct {
	backend grpc.ClientConnInterface
}

// HandleStream creates a new stream to the backend. Requests from
// incomingStream are forwarded to the backend stream and responses from the
// backend stream are sent back in the incomingStream.
func (s *forwardingStreamHandler) HandleStream(srv any, incomingStream grpc.ServerStream) error {
	// All gRPC invocations has a grpc.ServerTransportStream context.
	method, _ := grpc.Method(incomingStream.Context())
	desc := grpc.StreamDesc{
		// According to grpc.StreamDesc documentation, StreamName and Handler
		// are only used when registering handlers on a server.
		StreamName: "",
		Handler:    nil,
		// Streaming behaviour is wanted, single message is treated the same on
		// transport level, the application just closes the stream after the
		// first message.
		ServerStreams: true,
		ClientStreams: true,
	}
	ctx, cancel := context.WithCancelCause(incomingStream.Context())
	defer cancel(nil)

	// ctx is guaranteed to be canceled when returning from this method, so
	// outgoingStream will not leak resources.
	outgoingStream, err := s.backend.NewStream(ctx, &desc, method)
	if err != nil {
		return err
	}

	// The only way to cancel a blocking incomingStream.RecvMsg is to return
	// from this method. Therefore, an error from outgoingStream.RecvMsg
	// needs to be returned without waiting for incomingStream.RecvMsg, so
	// it cannot be run inside e.g. errgroup.Go.
	go func() {
		for {
			msg := &emptypb.Empty{}
			if err := incomingStream.RecvMsg(msg); err != nil {
				if err == io.EOF {
					// Let's continue to receive on outgoingStream, so don't
					// cancel grouptCtx.
					outgoingStream.CloseSend()
					return
				}
				// Cancel ctx immediately.
				cancel(err)
				return
			}
			if err := outgoingStream.SendMsg(msg); err != nil {
				if err == io.EOF {
					// The error will be returned by outgoingStream.RecvMsg(),
					// no need to cancel ctx now.
					return
				}
				// Cancel ctx immediately.
				cancel(err)
				return
			}
		}
	}()

	for {
		msg := &emptypb.Empty{}
		if err := outgoingStream.RecvMsg(msg); err != nil {
			if err != io.EOF {
				cancel(err)
			}
			break
		}
		if err := incomingStream.SendMsg(msg); err != nil {
			cancel(err)
			break
		}
	}
	return context.Cause(ctx)
}
