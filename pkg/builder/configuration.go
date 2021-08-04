package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/builder"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// NewDemultiplexingBuildQueueFromConfiguration creates a
// DemultiplexingBuildQueue that forwards traffic to schedulers
// specified in the configuration file.
func NewDemultiplexingBuildQueueFromConfiguration(schedulers map[string]*pb.SchedulerConfiguration, grpcClientFactory grpc.ClientFactory, nonExecutableInstanceNameAuthorizer auth.Authorizer) (BuildQueue, error) {
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
		if err := auth.AuthorizeSingleInstanceName(context.Background(), nonExecutableInstanceNameAuthorizer, instanceName); err != nil {
			return nil, digest.EmptyInstanceName, digest.EmptyInstanceName, util.StatusWrapf(err, "This instance name does not provide remote execution, nor can the caller be authorized to do remote caching")
		}
		// The instance name does not correspond to a
		// scheduler, but we should at least announce
		// its existence.
		//
		// This is used when bb_storage is set up to do
		// plain remote caching. Because we don't have a
		// scheduler, we need to handle GetCapabilities()
		// requests ourselves.
		return NonExecutableBuildQueue, instanceName, instanceName, nil
	}), nil
}
