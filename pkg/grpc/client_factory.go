package grpc

import (
	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"

	"google.golang.org/grpc"
)

// ClientFactory can be used to construct gRPC clients based on options
// specified in a configuration message.
type ClientFactory interface {
	NewClientFromConfiguration(configuration *configuration.ClientConfiguration, terminationGroup program.Group) (grpc.ClientConnInterface, error)
}
