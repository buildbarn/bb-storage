package builder

import (
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
func NewDemultiplexingBuildQueueFromConfiguration(schedulers map[string]*pb.SchedulerConfiguration, grpcClientFactory grpc.ClientFactory, nonExecutableInstanceNames digest.InstanceNameMatcher) (BuildQueue, error) {
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
			return nil, util.StatusWrapf(err, "Invalid instance name %#v: %s", k)
		}
		addInstanceNamePrefix, err := digest.NewInstanceName(scheduler.AddInstanceNamePrefix)
		if err != nil {
			return nil, util.StatusWrapf(err, "Invalid instance name %#v: %s", scheduler.AddInstanceNamePrefix)
		}
		endpoint, err := grpcClientFactory.NewClientFromConfiguration(scheduler.Endpoint)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failer to create scheduler RPC client for instance name %#v: ", k)
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

	return NewDemultiplexingBuildQueue(func(instanceName digest.InstanceName) (BuildQueue, digest.InstanceName, digest.InstanceName, error) {
		if idx := buildQueuesTrie.GetLongestPrefix(instanceName); idx >= 0 {
			// The instance name corresponds to a scheduler
			// to which we can forward requests.
			return buildQueues[idx].backend, buildQueues[idx].backendName, buildQueues[idx].instanceNamePatcher.PatchInstanceName(instanceName), nil
		}
		if nonExecutableInstanceNames(instanceName) {
			// The instance name does not correspond to a
			// scheduler, but we should at least announce
			// its existence.
			//
			// This is used when bb_storage is set up to do
			// plain remote caching. Because we don't have a
			// scheduler, we need to handle GetCapabilities()
			// requests ourselves.
			return NonExecutableBuildQueue, instanceName, instanceName, nil
		}
		return nil, digest.EmptyInstanceName, digest.EmptyInstanceName, status.Errorf(codes.InvalidArgument, "Unknown instance name")
	}), nil
}
