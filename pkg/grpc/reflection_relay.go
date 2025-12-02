package grpc

import (
	"context"
	"maps"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/program"
	grpcpb "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jhump/protoreflect/v2/grpcreflect"
	"github.com/jhump/protoreflect/v2/protoresolve"
	v1reflectiongrpc "google.golang.org/grpc/reflection/grpc_reflection_v1"
	v1alphareflectiongrpc "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type combinedServiceInfoProvider struct {
	server        reflection.ServiceInfoProvider
	extraServices map[string]grpc.ServiceInfo
}

var _ reflection.ServiceInfoProvider = (*combinedServiceInfoProvider)(nil)

// GetServiceInfo returns the currently available services, which might have
// changed since the creation of this reflection server.
func (p *combinedServiceInfoProvider) GetServiceInfo() map[string]grpc.ServiceInfo {
	services := make(map[string]grpc.ServiceInfo)
	maps.Copy(services, p.extraServices)
	maps.Copy(services, p.server.GetServiceInfo())
	return services
}

// registerReflection registers the google.golang.org/grpc/reflection/ service
// on a grpc.Server and calls remote backends in case for relayed services. The
// connections to the backend will run with the backendCtx.
func registerReflection(backendCtx context.Context, s *grpc.Server, serverRelayConfiguration []*grpcpb.ServerRelayConfiguration, group program.Group, grpcClientFactory ClientFactory) error {
	// Accumulate all the service names.
	relayServices := make(map[string]grpc.ServiceInfo)
	for _, relay := range serverRelayConfiguration {
		for _, serviceMethod := range relay.Methods {
			if !strings.HasPrefix(serviceMethod, "/") {
				return status.Errorf(codes.InvalidArgument, "Malformed service method name %q, should start with '/'", serviceMethod)
			}
			pos := strings.LastIndex(serviceMethod, "/")
			if pos == -1 || pos == 0 {
				return status.Errorf(codes.InvalidArgument, "Malformed name %q, expected '/' between service and method", serviceMethod)
			}
			serviceName := serviceMethod[1:pos]
			// According to ServiceInfoProvider docs for ServerOptions.Services,
			// the reflection service is only interested in the service names.
			relayServices[serviceName] = grpc.ServiceInfo{}
		}
	}

	// Make a combined descriptor and extension resolver.
	reflectionBackends := []protoresolve.Resolver{}
	for relayIdx, relay := range serverRelayConfiguration {
		grpcClient, err := grpcClientFactory.NewClientFromConfiguration(relay.Endpoint, group)
		if err != nil {
			return util.StatusWrapf(err, "Failed to create relay RPC client %d", relayIdx+1)
		}
		resolver := grpcreflect.NewClientAuto(backendCtx, grpcClient).AsResolver()
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
	v1reflectiongrpc.RegisterServerReflectionServer(s, reflection.NewServerV1(serverOptions))
	v1alphareflectiongrpc.RegisterServerReflectionServer(s, reflection.NewServer(serverOptions))
	return nil
}
