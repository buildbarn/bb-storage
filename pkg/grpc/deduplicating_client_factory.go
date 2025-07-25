package grpc

import (
	"sync"

	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
)

type deduplicatingClientFactory struct {
	base ClientFactory

	lock    sync.Mutex
	clients map[string]grpc.ClientConnInterface
}

// NewDeduplicatingClientFactory creates a decorator for ClientFactory
// that deduplicates requests for creating gRPC clients. This means that
// clients for identical endpoints, having identical TLS settings, etc.
// will not cause multiple connections to be established.
func NewDeduplicatingClientFactory(base ClientFactory) ClientFactory {
	return &deduplicatingClientFactory{
		base:    base,
		clients: map[string]grpc.ClientConnInterface{},
	}
}

func (cf *deduplicatingClientFactory) NewClientFromConfiguration(configuration *configuration.ClientConfiguration, group program.Group) (grpc.ClientConnInterface, error) {
	key := prototext.Format(configuration)
	cf.lock.Lock()
	defer cf.lock.Unlock()

	// Attempt to return an existing client.
	if client, ok := cf.clients[key]; ok {
		return client, nil
	}

	// Create a new client, as it has a different configuration.
	client, err := cf.base.NewClientFromConfiguration(configuration, group)
	if err != nil {
		return nil, err
	}
	cf.clients[key] = client
	return client, nil
}
