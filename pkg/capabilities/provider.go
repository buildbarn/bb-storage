package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Provider of capabilities.
//
// This interface is implemented for objects like BlobAccess and
// BuildQueue to report parts of REv2 ServerCapabilities messages. Each
// type is responsible for only reporting the fields that apply that
// subsystem. For example, BlobAccess will report CacheCapabilities,
// while BuildQueue will report ExecutionCapabilities. The messages
// returned by each subsystem can be merged into a complete message that
// can be returned to clients.
//
// All implementations of Provider must make sure that if
// GetCapabilities() succeeds, either CacheCapabilities or
// ExecutionCapabilities is set. The REv2 spec doesn't describe the case
// where both are unset.
type Provider interface {
	GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error)
}
