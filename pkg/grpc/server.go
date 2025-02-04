package grpc

import (
	"context"
	"net"
	"os"

	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/grpc-ecosystem/go-grpc-prometheus"

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

// NewServersFromConfigurationAndServe creates a series of gRPC servers
// based on a configuration stored in a list of Protobuf messages. It
// then lets all of these gRPC servers listen on the network addresses
// of UNIX socket paths provided.
func NewServersFromConfigurationAndServe(configurations []*configuration.ServerConfiguration, registrationFunc func(grpc.ServiceRegistrar), group program.Group, grpcClientFactory ClientFactory) error {
	for _, configuration := range configurations {
		// Create an authenticator for requests.
		authenticator, needsPeerTransportCredentials, requestTLSClientCertificate, err := NewAuthenticatorFromConfiguration(configuration.AuthenticationPolicy, group, grpcClientFactory)
		if err != nil {
			return err
		}

		// Default server options.
		unaryInterceptors := []grpc.UnaryServerInterceptor{
			grpc_prometheus.UnaryServerInterceptor,
			otelgrpc.UnaryServerInterceptor(),
			RequestMetadataTracingUnaryInterceptor,
		}
		streamInterceptors := []grpc.StreamServerInterceptor{
			grpc_prometheus.StreamServerInterceptor,
			otelgrpc.StreamServerInterceptor(),
			RequestMetadataTracingStreamInterceptor,
		}

		// Optional: Tracing attributes.
		if tracing := configuration.Tracing; len(tracing) > 0 {
			extractor := NewProtoTraceAttributesExtractor(tracing, util.DefaultErrorLogger)
			unaryInterceptors = append(unaryInterceptors, extractor.InterceptUnaryServer)
			streamInterceptors = append(streamInterceptors, extractor.InterceptStreamServer)
		}

		unaryInterceptors = append(unaryInterceptors, NewAuthenticatingUnaryInterceptor(authenticator))
		streamInterceptors = append(streamInterceptors, NewAuthenticatingStreamInterceptor(authenticator))

		serverOptions := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
			grpc.ChainStreamInterceptor(streamInterceptors...),
		}

		// Enable TLS transport credentials if provided.
		hasCredsOption := false
		if tlsConfig, err := util.NewTLSConfigFromServerConfiguration(configuration.Tls, requestTLSClientCertificate); err != nil {
			return err
		} else if tlsConfig != nil {
			hasCredsOption = true
			serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
		}

		// Enable UNIX socket peer credentials if used in the
		// authenticator configuration.
		if needsPeerTransportCredentials {
			if hasCredsOption {
				return status.Error(codes.InvalidArgument, "Peer credentials authentication and TLS cannot be enabled at the same time")
			}
			serverOptions = append(serverOptions, grpc.Creds(PeerTransportCredentials))
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

		if keepaliveParams := configuration.KeepaliveParameters; keepaliveParams != nil {
			maxConnectionIdle := keepaliveParams.MaxConnectionIdle
			if err := maxConnectionIdle.CheckValid(); err != nil {
				return util.StatusWrap(err, "Failed to parse keepalive server parameter max connection idle")
			}
			maxConnectionAge := keepaliveParams.MaxConnectionAge
			if err := maxConnectionAge.CheckValid(); err != nil {
				return util.StatusWrap(err, "Failed to parse keepalive server parameter max connection age")
			}
			maxConnectionAgeGrace := keepaliveParams.MaxConnectionAgeGrace
			if err := maxConnectionAgeGrace.CheckValid(); err != nil {
				return util.StatusWrap(err, "Failed to parse keepalive server parameter max connection age grace")
			}
			time := keepaliveParams.Time
			if err := time.CheckValid(); err != nil {
				return util.StatusWrap(err, "Failed to parse keepalive server parameter time")
			}
			timeout := keepaliveParams.Timeout
			if err := timeout.CheckValid(); err != nil {
				return util.StatusWrap(err, "Failed to parse keepalive server parameter timeout")
			}
			serverOptions = append(serverOptions, grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle:     maxConnectionIdle.AsDuration(),
				MaxConnectionAge:      maxConnectionAge.AsDuration(),
				MaxConnectionAgeGrace: maxConnectionAgeGrace.AsDuration(),
				Time:                  time.AsDuration(),
				Timeout:               timeout.AsDuration(),
			}))
		}

		// Create server.
		s := grpc.NewServer(serverOptions...)
		stopFunc := s.Stop
		if configuration.StopGracefully {
			stopFunc = s.GracefulStop
		}
		group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			<-ctx.Done()
			stopFunc()
			return nil
		})
		registrationFunc(s)

		// Enable default services.
		grpc_prometheus.Register(s)
		reflection.Register(s)
		h := health.NewServer()
		grpc_health_v1.RegisterHealthServer(s, h)
		// TODO: Construct an API for the caller to indicate
		// when it is healthy and set this.
		h.SetServingStatus(configuration.HealthCheckService, grpc_health_v1.HealthCheckResponse_SERVING)

		if len(configuration.ListenAddresses)+len(configuration.ListenPaths) == 0 {
			return status.Error(codes.InvalidArgument, "GRPC server configured without any listen addresses or paths")
		}

		// TCP sockets.
		for _, listenAddressIter := range configuration.ListenAddresses {
			listenAddress := listenAddressIter
			sock, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return util.StatusWrapf(err, "Failed to create listening socket for %#v", listenAddress)
			}
			group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				if err := s.Serve(sock); err != nil {
					return util.StatusWrapf(err, "gRPC server failed for %#v", listenAddress)
				}
				return nil
			})
		}

		// UNIX sockets.
		for _, listenPathIter := range configuration.ListenPaths {
			listenPath := listenPathIter
			if err := os.Remove(listenPath); err != nil && !os.IsNotExist(err) {
				return util.StatusWrapf(err, "Could not remove stale socket %#v", listenPath)
			}
			sock, err := net.Listen("unix", listenPath)
			if err != nil {
				return util.StatusWrapf(err, "Failed to create listening socket for %#v", listenPath)
			}
			group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				if err := s.Serve(sock); err != nil {
					return util.StatusWrapf(err, "gRPC server failed for %#v", listenPath)
				}
				return nil
			})
		}
	}
	return nil
}
