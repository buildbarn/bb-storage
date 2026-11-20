package grpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
)

type deduplicatingClientFactory struct {
	base ClientFactory

	lock    sync.Mutex
	clients map[string]*deduplicatedClient
}

// NewDeduplicatingClientFactory creates a decorator for ClientFactory
// that deduplicates requests for creating gRPC clients. This means that
// clients for identical endpoints, having identical TLS settings, etc.
// will not cause multiple connections to be established.
func NewDeduplicatingClientFactory(base ClientFactory) ClientFactory {
	return &deduplicatingClientFactory{
		base:    base,
		clients: map[string]*deduplicatedClient{},
	}
}

func (cf *deduplicatingClientFactory) NewClientFromConfiguration(configuration *configuration.ClientConfiguration, group program.Group) (grpc.ClientConnInterface, error) {
	key := prototext.Format(configuration)
	cf.lock.Lock()
	defer cf.lock.Unlock()

	// Attempt to return an existing client.
	client := cf.clients[key]
	if client == nil {
		// Create a new client, as it has a different configuration.
		var err error
		client, err = newDeduplicatedClient(cf.base, configuration)
		if err != nil {
			return nil, err
		}
		cf.clients[key] = client
	} else {
		client.waitGroup.Add(1)
	}
	// Apply reference counting during the group lifetime.
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		<-ctx.Done()
		client.waitGroup.Done()
		return nil
	})
	return client.connection, nil
}

type deduplicatedClient struct {
	connection grpc.ClientConnInterface
	waitGroup  sync.WaitGroup
}

// newDeduplicatedClient creates a gRPC connection that is closed
// when the returned waitGroup gets down to zero. On success, the
// waitGroup has already been incremented once to avoid immediate
// teardown..
func newDeduplicatedClient(clientFactory ClientFactory, configuration *configuration.ClientConfiguration) (*deduplicatedClient, error) {
	client := &deduplicatedClient{
		connection: nil,
		waitGroup:  sync.WaitGroup{},
	}
	// Keep alive until this function returns.
	client.waitGroup.Add(1)
	// Create a new independent context that doesn't finish until
	// all future deduplicated connections have finished.
	setupErrChan := make(chan error, 1)
	go func() {
		if err := program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			var err error
			client.connection, err = clientFactory.NewClientFromConfiguration(configuration, siblingsGroup)
			setupErrChan <- err
			// Wait for all future deduplicated connections to
			// finish before the siblings context is cancelled.
			client.waitGroup.Wait()
			return nil
		}); err != nil {
			panic(fmt.Sprintf("The deduplicating Go routine returns nil, not %v", err))
		}
	}()
	if err := <-setupErrChan; err != nil {
		// Setup failed so release the connection.
		client.waitGroup.Done()
		return nil, err
	}
	return client, nil
}
