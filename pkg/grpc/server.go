package grpc

import (
	"context"
	"net"
	"os"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

func init() {
	// Add Prometheus timing metrics.
	grpc_prometheus.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets(
			util.DecimalExponentialBuckets(-3, 6, 2)))
}

// ServerGroup holds references to multiple grpc.Server instances, to be
// able to set the readiness status.
type ServerGroup struct {
	grpcServers []*grpc.Server

	healthServers       []*health.Server
	healthCheckServices []string
}

// NewServersFromConfigurationAndServe creates a series of gRPC servers
// based on a configuration stored in a list of Protobuf messages. It
// then lets all of these gRPC servers listen on the network addresses
// of UNIX socket paths provided.
//
// This function returns immediately after initializing and registering the
// serving routines in the terminationContext, terminationGroup.
func NewServersFromConfigurationAndServe(terminationContext context.Context, terminationGroup *errgroup.Group, configurations []*configuration.ServerConfiguration, registrationFunc func(grpc.ServiceRegistrar)) (*ServerGroup, error) {
	sg := &ServerGroup{}
	if err := sg.startup(terminationContext, terminationGroup, configurations, registrationFunc); err != nil {
		return nil, util.StatusWrap(err, "gRPC server failure")
	}
	terminationGroup.Go(func() error {
		<-terminationContext.Done()
		sg.shutdown()
		return nil
	})
	return sg, nil
}

func (sg *ServerGroup) startup(terminationContext context.Context, terminationGroup *errgroup.Group, configurations []*configuration.ServerConfiguration, registrationFunc func(grpc.ServiceRegistrar)) error {
	for _, configuration := range configurations {
		// Create an authenticator for requests.
		authenticator, err := NewAuthenticatorFromConfiguration(configuration.AuthenticationPolicy)
		if err != nil {
			return err
		}

		// Default server options.
		serverOptions := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(
				grpc_prometheus.UnaryServerInterceptor,
				otelgrpc.UnaryServerInterceptor(),
				RequestMetadataTracingUnaryInterceptor,
				NewAuthenticatingUnaryInterceptor(authenticator)),
			grpc.ChainStreamInterceptor(
				grpc_prometheus.StreamServerInterceptor,
				otelgrpc.StreamServerInterceptor(),
				RequestMetadataTracingStreamInterceptor,
				NewAuthenticatingStreamInterceptor(authenticator)),
		}

		// Enable TLS if provided.
		if tlsConfig, err := util.NewTLSConfigFromServerConfiguration(configuration.Tls); err != nil {
			return err
		} else if tlsConfig != nil {
			serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
		}

		if maxRecvMsgSize := configuration.MaximumReceivedMessageSizeBytes; maxRecvMsgSize != 0 {
			serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(int(maxRecvMsgSize)))
		}

		if windowSize := configuration.InitialWindowSizeBytes; windowSize != 0 {
			serverOptions = append(serverOptions, grpc.InitialWindowSize(windowSize))
		}
		if connWindowSize := configuration.InitialConnWindowSizeBytes; connWindowSize != 0 {
			serverOptions = append(serverOptions, grpc.InitialConnWindowSize(connWindowSize))
		}

		// Optional: Keepalive enforcement policy.
		if policy := configuration.KeepaliveEnforcementPolicy; policy != nil {
			minTime := policy.MinTime
			if err := minTime.CheckValid(); err != nil {
				return util.StatusWrap(err, "Failed to parse keepalive enforcement policy minimum time")
			}
			serverOptions = append(serverOptions, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             minTime.AsDuration(),
				PermitWithoutStream: policy.PermitWithoutStream,
			}))
		}

		// Create server.
		s := grpc.NewServer(serverOptions...)
		sg.grpcServers = append(sg.grpcServers, s)
		registrationFunc(s)

		// Enable default services.
		grpc_prometheus.Register(s)
		reflection.Register(s)
		h := health.NewServer()
		sg.healthServers = append(sg.healthServers, h)
		sg.healthCheckServices = append(sg.healthCheckServices, configuration.HealthCheckService)
		grpc_health_v1.RegisterHealthServer(s, h)
		// TODO: Construct an API for the caller to indicate
		// when it is healthy and set this.
		h.SetServingStatus(configuration.HealthCheckService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		if len(configuration.ListenAddresses)+len(configuration.ListenPaths) == 0 {
			return status.Error(codes.InvalidArgument, "GRPC server configured without any listen addresses or paths")
		}

		// TCP sockets.
		for _, listenAddress := range configuration.ListenAddresses {
			sock, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return util.StatusWrapf(err, "Failed to create listening socket for %#v", listenAddress)
			}
			terminationGroup.Go(func() error {
				if err := s.Serve(sock); err != nil {
					return util.StatusWrap(err, "gRPC server failure")
				}
				return nil
			})
		}

		// UNIX sockets.
		for _, listenPath := range configuration.ListenPaths {
			if err := os.Remove(listenPath); err != nil && !os.IsNotExist(err) {
				return util.StatusWrapf(err, "Could not remove stale socket %#v", listenPath)
			}
			sock, err := net.Listen("unix", listenPath)
			if err != nil {
				return util.StatusWrapf(err, "Failed to create listening socket for %#v", listenPath)
			}
			terminationGroup.Go(func() error {
				if err := s.Serve(sock); err != nil {
					return util.StatusWrap(err, "gRPC server failure")
				}
				return nil
			})
		}
	}
	return nil
}

// SetReady marks the gRPC health service to respond with
// grpc_health_v1.HealthCheckResponse_SERVING.
func (sg *ServerGroup) SetReady() {
	for i, h := range sg.healthServers {
		h.SetServingStatus(sg.healthCheckServices[i], grpc_health_v1.HealthCheckResponse_SERVING)
	}
}

// Shutdown stops the gRPC services. It cancels all active RPCs. Clients are
// supposed to retry the operations whenever the service is up again.
func (sg *ServerGroup) shutdown() {
	for _, h := range sg.healthServers {
		h.Shutdown()
	}
	for _, s := range sg.grpcServers {
		s.Stop()
	}
}
