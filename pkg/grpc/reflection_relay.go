package grpc

import (
	"context"
	"maps"

	"github.com/jhump/protoreflect/v2/grpcreflect"
	"github.com/jhump/protoreflect/v2/protoresolve"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
)

type combinedServiceInfoProvider struct {
	server        reflection.ServiceInfoProvider
	extraServices map[string]grpc.ServiceInfo
}

var _ reflection.ServiceInfoProvider = (*combinedServiceInfoProvider)(nil)

// GetServiceInfo returns the currently available services, which might have
// changed since the creation of this reflection server.
func (p *combinedServiceInfoProvider) GetServiceInfo() map[string]grpc.ServiceInfo {
	serverServiceInfo := p.server.GetServiceInfo()
	services := make(map[string]grpc.ServiceInfo, len(p.extraServices)+len(serverServiceInfo))
	maps.Copy(services, p.extraServices)
	maps.Copy(services, serverServiceInfo)
	return services
}

// registerReflectionServer registers the google.golang.org/grpc/reflection/
// service on a grpc.Server and calls remote backends in case for relayed
// services. The connections to the backend will run with the backendCtx.
func registerReflectionServer(backendCtx context.Context, s *grpc.Server, serverRelayConfigurations []serverRelayConfigWithGrpcClient) error {
	// Accumulate all the service names.
	relayServices := make(map[string]grpc.ServiceInfo)
	for _, relay := range serverRelayConfigurations {
		for _, service := range relay.config.Services {
			// According to ServiceInfoProvider docs for ServerOptions.Services,
			// the reflection service is only interested in the service names.
			relayServices[service] = grpc.ServiceInfo{}
		}
	}

	// Make a combined descriptor and extension resolver.
	reflectionBackends := []protoresolve.Resolver{}
	for _, relay := range serverRelayConfigurations {
		resolver := grpcreflect.NewClientAuto(backendCtx, relay.grpcClient).AsResolver()
		reflectionBackends = append(reflectionBackends, resolver)
	}
	combinedRemoteResolver := protoresolve.Combine(reflectionBackends...)

	serverOptions := reflection.ServerOptions{
		Services: &combinedServiceInfoProvider{
			server:        s,
			extraServices: relayServices,
		},
		DescriptorResolver: combinedRemoteResolver,
		ExtensionResolver:  protoresolve.TypesFromDescriptorPool(combinedRemoteResolver),
	}
	grpc_reflection_v1.RegisterServerReflectionServer(s, reflection.NewServerV1(serverOptions))
	return nil
}
