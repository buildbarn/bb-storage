package grpc

import (
	"io"

	"golang.org/x/sync/errgroup"
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
	method := grpc.ServerTransportStreamFromContext(incomingStream.Context()).Method()
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
	group, groupCtx := errgroup.WithContext(incomingStream.Context())
	group.Go(func() error {
		// groupCtx is guaranteed to be canceled before returning from this method, so outgoingStream will not leak resources.
		outgoingStream, err := s.backend.NewStream(groupCtx, &desc, method)
		if err != nil {
			return err
		}
		// Avoid group.Go because incomingStream.RecvMsg might block returning
		// an error from the outgoingStream and getting the context for
		// incomingStream canceled.
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
					// Cancel groupCtx immediately.
					group.Go(func() error { return err })
					return
				}
				if err := outgoingStream.SendMsg(msg); err != nil {
					if err == io.EOF {
						// The error will be returned by outgoingStream.RecvMsg(),
						// no need to cancel groupCtx now.
						return
					}
					// Cancel groupCtx immediately.
					group.Go(func() error { return err })
					return
				}
			}
		}()

		for {
			msg := &emptypb.Empty{}
			if err := outgoingStream.RecvMsg(msg); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			if err := incomingStream.SendMsg(msg); err != nil {
				return err
			}
		}
	})
	return group.Wait()
}
