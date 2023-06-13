package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/builder"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewDemultiplexingBuildQueueFromConfiguration creates a
// DemultiplexingBuildQueue that forwards traffic to schedulers
// specified in the configuration file.
func NewDemultiplexingBuildQueueFromConfiguration(schedulers map[string]*pb.SchedulerConfiguration, grpcClientFactory grpc.ClientFactory) (BuildQueue, error) {
	buildQueuesTrie := digest.NewInstanceNameTrie()
	type buildQueueInfo struct {
		backend             BuildQueue
		backendName         digest.InstanceName
		instanceNamePatcher digest.InstanceNamePatcher
	}
	var buildQueues []buildQueueInfo
	for k, scheduler := range schedulers {
		matchInstanceNamePrefix, err := digest.NewInstanceName(k)
		if err != nil {
			return nil, util.StatusWrapf(err, "Invalid instance name %#v", k)
		}
		addInstanceNamePrefix, err := digest.NewInstanceName(scheduler.AddInstanceNamePrefix)
		if err != nil {
			return nil, util.StatusWrapf(err, "Invalid instance name %#v", scheduler.AddInstanceNamePrefix)
		}
		endpoint, err := grpcClientFactory.NewClientFromConfiguration(scheduler.Endpoint)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to create scheduler RPC client for instance name %#v", k)
		}
		buildQueuesTrie.Set(matchInstanceNamePrefix, len(buildQueues))
		buildQueues = append(buildQueues, buildQueueInfo{
			backend:     NewForwardingBuildQueue(endpoint),
			backendName: matchInstanceNamePrefix,
			instanceNamePatcher: digest.NewInstanceNamePatcher(
				matchInstanceNamePrefix,
				addInstanceNamePrefix),
		})
	}

	return NewDemultiplexingBuildQueue(func(ctx context.Context, instanceName digest.InstanceName) (BuildQueue, digest.InstanceName, digest.InstanceName, error) {
		idx := buildQueuesTrie.GetLongestPrefix(instanceName)
		if idx < 0 {
			return nil, digest.EmptyInstanceName, digest.EmptyInstanceName, status.Errorf(codes.InvalidArgument, "Unknown instance name: %#v", instanceName.String())
		}
		return buildQueues[idx].backend, buildQueues[idx].backendName, buildQueues[idx].instanceNamePatcher.PatchInstanceName(instanceName), nil
	}), nil
}
