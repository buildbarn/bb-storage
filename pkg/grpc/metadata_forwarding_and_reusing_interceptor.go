package grpc

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type headerToSave struct {
	index  int
	values []string
}

// MetadataForwardingAndReusingInterceptor is a gRPC interceptor that
// extracts a set of incoming metadata headers from the calling context
// and copies them to the outgoing metadata headers.
//
// In addition to that, these headers are preserved and are attached to
// future calls that don't provide these headers. This option is useful
// when bb_storage is used as a personal proxy, where clients (e.g.,
// Bazel) can be used to inject credentials into bb_storage.
type MetadataForwardingAndReusingInterceptor struct {
	headers []string

	lock        sync.Mutex
	savedValues [][]string
}

// NewMetadataForwardingAndReusingInterceptor creates a
// MetadataForwardingAndReusingInterceptor that forwards a provided set
// of headers.
func NewMetadataForwardingAndReusingInterceptor(headers []string) *MetadataForwardingAndReusingInterceptor {
	return &MetadataForwardingAndReusingInterceptor{
		headers:     headers,
		savedValues: make([][]string, len(headers)),
	}
}

func (i *MetadataForwardingAndReusingInterceptor) prepareContext(ctx context.Context) (context.Context, []headerToSave) {
	var headerValues MetadataHeaderValues
	headersToSave := make([]headerToSave, 0, len(i.headers))
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		// Call from within the context of an incoming gRPC call.
		headersToReuse := make([]int, 0, len(i.headers))
		for index, header := range i.headers {
			if values := md.Get(header); len(values) > 0 {
				// Client provided header values. Use those.
				headersToSave = append(headersToSave, headerToSave{
					index:  index,
					values: values,
				})
				headerValues.Add(header, values)
			} else {
				// Client did not provide header values.
				// Reuse header values from previous calls.
				headersToReuse = append(headersToReuse, index)
			}
		}

		if len(headersToReuse) > 0 {
			i.lock.Lock()
			for _, index := range headersToReuse {
				headerValues.Add(i.headers[index], i.savedValues[index])
			}
			i.lock.Unlock()
		}
	} else {
		// Call outside the context of an incoming gRPC call.
		// Reuse all header values from previous calls.
		i.lock.Lock()
		for index, header := range i.headers {
			headerValues.Add(header, i.savedValues[index])
		}
		i.lock.Unlock()
	}
	return metadata.AppendToOutgoingContext(ctx, headerValues...), headersToSave
}

func (i *MetadataForwardingAndReusingInterceptor) saveHeaders(headersToSave []headerToSave) {
	if len(headersToSave) > 0 {
		i.lock.Lock()
		for _, headerToSave := range headersToSave {
			i.savedValues[headerToSave.index] = headerToSave.values
		}
		i.lock.Unlock()
	}
}

// InterceptUnaryClient can be used as an interceptor for unary client
// gRPC calls.
func (i *MetadataForwardingAndReusingInterceptor) InterceptUnaryClient(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, headersToSave := i.prepareContext(ctx)
	if err := invoker(ctx, method, req, resp, cc, opts...); err != nil {
		return err
	}
	i.saveHeaders(headersToSave)
	return nil
}

var _ grpc.UnaryClientInterceptor = (*MetadataForwardingAndReusingInterceptor)(nil).InterceptUnaryClient

// InterceptStreamClient can be used as an interceptor for streaming
// client gRPC calls.
func (i *MetadataForwardingAndReusingInterceptor) InterceptStreamClient(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// TODO: Figure out a way to reliably determine whether a
	// streaming RPC has completed successfully. That would allow us
	// to save the headers that were used. The ClientStream
	// interface seems to be too weak to do that reliably.
	//
	// More details: https://stackoverflow.com/questions/42988396/whats-the-best-way-to-determine-when-an-rpc-session-ends-using-a-streamclientin
	ctx, _ = i.prepareContext(ctx)
	return streamer(ctx, desc, cc, method, opts...)
}

var _ grpc.StreamClientInterceptor = (*MetadataForwardingAndReusingInterceptor)(nil).InterceptStreamClient
