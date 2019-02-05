package builder

import (
	"context"
	"fmt"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BuildQueueGetter is the callback invoked by the demultiplexing build
// queue to obtain a backend that matches the instance name that is
// provided.
type BuildQueueGetter func(instanceName string) (BuildQueue, error)

type demultiplexingBuildQueue struct {
	buildQueueGetter BuildQueueGetter
}

// NewDemultiplexingBuildQueue creates an adapter for the Execution
// service to forward requests to different backends backed on the
// instance given in requests. Job identifiers returned by backends are
// prefixed with the instance name, so that successive requests may
// demultiplex the requests later on.
func NewDemultiplexingBuildQueue(buildQueueGetter BuildQueueGetter) BuildQueue {
	return &demultiplexingBuildQueue{
		buildQueueGetter: buildQueueGetter,
	}
}

func (bq *demultiplexingBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	if strings.ContainsRune(in.InstanceName, '|') {
		return nil, status.Errorf(codes.InvalidArgument, "Instance name cannot contain a pipe character")
	}
	backend, err := bq.buildQueueGetter(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to obtain backend for instance %#v", in.InstanceName)
	}
	return backend.GetCapabilities(ctx, in)
}

func (bq *demultiplexingBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	if strings.ContainsRune(in.InstanceName, '|') {
		return status.Errorf(codes.InvalidArgument, "Instance name cannot contain a pipe character")
	}
	backend, err := bq.buildQueueGetter(in.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain backend for instance %#v", in.InstanceName)
	}
	return backend.Execute(in, &operationNamePrepender{
		Execution_ExecuteServer: out,
		prefix:                  in.InstanceName,
	})
}

func (bq *demultiplexingBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	target := strings.SplitN(in.Name, "|", 2)
	if len(target) != 2 {
		return status.Errorf(codes.InvalidArgument, "Unable to extract instance from operation name")
	}
	backend, err := bq.buildQueueGetter(target[0])
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain backend for instance %#v", target[0])
	}
	requestCopy := *in
	requestCopy.Name = target[1]
	return backend.WaitExecution(in, &operationNamePrepender{
		Execution_ExecuteServer: out,
		prefix:                  target[1],
	})
}

type operationNamePrepender struct {
	remoteexecution.Execution_ExecuteServer
	prefix string
}

func (np *operationNamePrepender) Send(operation *longrunning.Operation) error {
	operationCopy := *operation
	operationCopy.Name = fmt.Sprintf("%s|%s", np.prefix, operation.Name)
	return np.Execution_ExecuteServer.Send(&operationCopy)
}
