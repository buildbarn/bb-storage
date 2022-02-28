package builder

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
)

// BuildQueue is an interface for the set of operations that a scheduler
// process must implement.
type BuildQueue interface {
	capabilities.Provider
	remoteexecution.ExecutionServer
}
