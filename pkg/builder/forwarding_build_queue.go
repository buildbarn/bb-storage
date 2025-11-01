package builder

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type forwardingBuildQueue struct {
	capabilitiesClient remoteexecution.CapabilitiesClient
	executionClient    remoteexecution.ExecutionClient
}

// NewForwardingBuildQueue creates a GRPC service for the Capabilities and
// Execution service that simply forwards all requests to a GRPC client. This
// may be used by the frontend processes to forward execution requests to
// scheduler processes in unmodified form.
//
// Details: https://github.com/grpc/grpc-go/issues/2297
func NewForwardingBuildQueue(client grpc.ClientConnInterface) BuildQueue {
	return &forwardingBuildQueue{
		capabilitiesClient: remoteexecution.NewCapabilitiesClient(client),
		executionClient:    remoteexecution.NewExecutionClient(client),
	}
}

func (bq *forwardingBuildQueue) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	serverCapabilities, err := bq.capabilitiesClient.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
		InstanceName: instanceName.String(),
	})
	if err != nil {
		return nil, err
	}
	if executionCapabilities := serverCapabilities.ExecutionCapabilities; executionCapabilities != nil {
		return &remoteexecution.ServerCapabilities{
			ExecutionCapabilities: executionCapabilities,
			DeprecatedApiVersion:  serverCapabilities.DeprecatedApiVersion,
			LowApiVersion:         serverCapabilities.LowApiVersion,
			HighApiVersion:        serverCapabilities.HighApiVersion,
		}, nil
	}
	return nil, status.Errorf(codes.InvalidArgument, "Instance name %#v does not support remote execution", instanceName.String())
}

func forwardOperations(cancel context.CancelFunc, client remoteexecution.Execution_ExecuteClient, server remoteexecution.Execution_ExecuteServer) error {
	for {
		operation, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := server.Send(operation); err != nil {
			// Failed to forward response to the caller.
			// Cancel the operation and drain any pending
			// responses.
			cancel()
			for {
				if _, err := client.Recv(); err != nil {
					break
				}
			}
			return err
		}
	}
}

func (bq *forwardingBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	ctx, cancel := context.WithCancel(out.Context())
	defer cancel()
	client, err := bq.executionClient.Execute(ctx, in)
	if err != nil {
		return err
	}
	return forwardOperations(cancel, client, out)
}

func (bq *forwardingBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	ctx, cancel := context.WithCancel(out.Context())
	defer cancel()
	client, err := bq.executionClient.WaitExecution(ctx, in)
	if err != nil {
		return err
	}
	return forwardOperations(cancel, client, out)
}
