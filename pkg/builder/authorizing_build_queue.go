package builder

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// NewAuthorizingBuildQueue creates a BuildQueue which authorizes
// Execute requests.
// Note that WaitExecution requests are not authorized,
// as their instance name is not known.
func NewAuthorizingBuildQueue(backend BuildQueue, authorizer auth.Authorizer) BuildQueue {
	return &authorizingBuildQueue{
		Provider: capabilities.NewAuthorizingProvider(backend, authorizer),

		backend:    backend,
		authorizer: authorizer,
	}
}

type authorizingBuildQueue struct {
	capabilities.Provider

	backend    BuildQueue
	authorizer auth.Authorizer
}

func (bq *authorizingBuildQueue) Execute(request *remoteexecution.ExecuteRequest, server remoteexecution.Execution_ExecuteServer) error {
	instanceName, err := digest.NewInstanceName(request.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", request.InstanceName)
	}
	if err := auth.AuthorizeSingleInstanceName(server.Context(), bq.authorizer, instanceName); err != nil {
		return util.StatusWrapf(err, "Failed to authorize to Execute() against instance name %#v", instanceName.String())
	}
	return bq.backend.Execute(request, server)
}

func (bq *authorizingBuildQueue) WaitExecution(request *remoteexecution.WaitExecutionRequest, server remoteexecution.Execution_WaitExecutionServer) error {
	return bq.backend.WaitExecution(request, server)
}
