package grpc

import (
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"

	"google.golang.org/grpc"
)

// ClientFactory can be used to construct gRPC clients based on options
// specified in a configuration message.
type ClientFactory interface {
	NewClientFromConfiguration(configuration *configuration.ClientConfiguration) (grpc.ClientConnInterface, error)
}

// DefaultClientFactory is an instance of ClientFactory that can be used
// to create gRPC client connections. All of the clients returned by
// this factory connect to their backend lazily. They are also
// deduplicated if multiple calls for the same configuration are made.
var DefaultClientFactory = NewDeduplicatingClientFactory(NewBaseClientFactory(NewLazyClientDialer(BaseClientDialer)))
