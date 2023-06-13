package builder

import (
	"context"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DemultiplexedBuildQueueGetter is the callback invoked by the
// demultiplexing build queue to obtain a backend that matches the
// instance name that is provided.
type DemultiplexedBuildQueueGetter func(ctx context.Context, instanceName digest.InstanceName) (BuildQueue, digest.InstanceName, digest.InstanceName, error)

type demultiplexingBuildQueue struct {
	getBackend DemultiplexedBuildQueueGetter
}

const demultiplexingSeparator = "/operations/"

// NewDemultiplexingBuildQueue creates an adapter for the Execution
// service to forward requests to different backends based on the
// instance name given in requests.
//
// Job identifiers returned by backends are prefixed with the instance
// name, so that successive requests may demultiplex the requests later
// on. The string "/operations/" is used as a separator between the
// instance name and the original operation name, as instance names are
// guaranteed to not contain that string.
func NewDemultiplexingBuildQueue(getBackend DemultiplexedBuildQueueGetter) BuildQueue {
	return &demultiplexingBuildQueue{
		getBackend: getBackend,
	}
}

func (bq *demultiplexingBuildQueue) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	backend, _, newInstanceName, err := bq.getBackend(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to obtain backend for instance name %#v", instanceName.String())
	}
	return backend.GetCapabilities(ctx, newInstanceName)
}

func (bq *demultiplexingBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	backend, backendName, newInstanceName, err := bq.getBackend(out.Context(), instanceName)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain backend for instance name %#v", instanceName.String())
	}

	var requestCopy remoteexecution.ExecuteRequest
	proto.Merge(&requestCopy, in)
	requestCopy.InstanceName = newInstanceName.String()
	return backend.Execute(&requestCopy, &operationNamePrepender{
		Execution_ExecuteServer: out,
		prefix:                  backendName.String() + demultiplexingSeparator,
	})
}

func (bq *demultiplexingBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	target := strings.SplitN(in.Name, demultiplexingSeparator, 2)
	if len(target) != 2 {
		return status.Errorf(codes.InvalidArgument, "Unable to extract instance name from operation name")
	}
	instanceName, err := digest.NewInstanceName(target[0])
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", target[0])
	}
	backend, _, _, err := bq.getBackend(out.Context(), instanceName)
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain backend for instance name %#v", instanceName.String())
	}

	var requestCopy remoteexecution.WaitExecutionRequest
	proto.Merge(&requestCopy, in)
	requestCopy.Name = target[1]
	return backend.WaitExecution(&requestCopy, &operationNamePrepender{
		Execution_ExecuteServer: out,
		prefix:                  target[0] + demultiplexingSeparator,
	})
}

type operationNamePrepender struct {
	remoteexecution.Execution_ExecuteServer
	prefix string
}

func (np *operationNamePrepender) Send(operation *longrunning.Operation) error {
	var operationCopy longrunning.Operation
	proto.Merge(&operationCopy, operation)
	operationCopy.Name = np.prefix + operation.Name
	return np.Execution_ExecuteServer.Send(&operationCopy)
}
