package builder

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/grpc"

	"go.opencensus.io/trace"
)

type forwardingBuildQueue struct {
	capabilitiesClient remoteexecution.CapabilitiesClient
	executionClient    remoteexecution.ExecutionClient
}

// NewForwardingBuildQueue creates a GRPC service for the Capbilities and
// Execution service that simply forwards all requests to a GRPC client. This
// may be used by the frontend processes to forward execution requests to
// scheduler processes in unmodified form.
//
// Details: https://github.com/grpc/grpc-go/issues/2297
func NewForwardingBuildQueue(client *grpc.ClientConn) BuildQueue {
	return &forwardingBuildQueue{
		capabilitiesClient: remoteexecution.NewCapabilitiesClient(client),
		executionClient:    remoteexecution.NewExecutionClient(client),
	}
}

func (bq *forwardingBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	ctx, span := trace.StartSpan(ctx, "buildqueue.Forwarding.GetCapabilities")
	defer span.End()

	return bq.capabilitiesClient.GetCapabilities(ctx, in)
}

func forwardOperations(client remoteexecution.Execution_ExecuteClient, server remoteexecution.Execution_ExecuteServer) error {
	for {
		operation, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := server.Send(operation); err != nil {
			return err
		}
	}
}

func (bq *forwardingBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	ctx, span := trace.StartSpan(out.Context(), "buildqueue.Forwarding.Execute")
	defer span.End()

	client, err := bq.executionClient.Execute(ctx, in)
	if err != nil {
		return err
	}
	return forwardOperations(client, out)
}

func (bq *forwardingBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	ctx, span := trace.StartSpan(out.Context(), "buildqueue.Forwarding.WaitExecution")
	defer span.End()

	client, err := bq.executionClient.WaitExecution(ctx, in)
	if err != nil {
		return err
	}
	return forwardOperations(client, out)
}
