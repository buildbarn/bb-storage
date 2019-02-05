package builder

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// BuildQueue is an interface for the set of operations that a scheduler
// process must implement.
type BuildQueue interface {
	remoteexecution.CapabilitiesServer
	remoteexecution.ExecutionServer
}
