package grpc

import (
	"net"
	"os"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"go.opencensus.io/plugin/ocgrpc"
)

func init() {
	// Add Prometheus timing metrics.
	grpc_prometheus.EnableClientHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets(
			util.DecimalExponentialBuckets(-3, 6, 2)))
	grpc_prometheus.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets(
			util.DecimalExponentialBuckets(-3, 6, 2)))
}

// NewGRPCClientFromConfiguration creates a gRPC client based on a
// configuration stored in a Protobuf message. This Protobuf message is
// used within all configuration files of Buildbarn applications.
func NewGRPCClientFromConfiguration(configuration *configuration.ClientConfiguration) (*grpc.ClientConn, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No gRPC client configuration provided")
	}

	var dialOptions []grpc.DialOption
	unaryInterceptors := []grpc.UnaryClientInterceptor{
		grpc_prometheus.UnaryClientInterceptor,
	}
	streamInterceptors := []grpc.StreamClientInterceptor{
		grpc_prometheus.StreamClientInterceptor,
	}

	// Optional: TLS.
	tlsConfig, err := util.NewTLSConfigFromClientConfiguration(configuration.Tls)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create TLS configuration")
	}
	if tlsConfig != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	// Optional: Keepalive.
	if configuration.Keepalive != nil {
		time, err := ptypes.Duration(configuration.Keepalive.Time)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse keepalive time")
		}
		timeout, err := ptypes.Duration(configuration.Keepalive.Timeout)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse keepalive timeout")
		}
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time,
			Timeout:             timeout,
			PermitWithoutStream: configuration.Keepalive.PermitWithoutStream,
		}))
	}

	// Optional: metadata forwarding.
	if headers := configuration.ForwardMetadata; len(headers) > 0 {
		unaryInterceptors = append(
			unaryInterceptors,
			NewMetadataForwardingUnaryClientInterceptor(headers))
		streamInterceptors = append(
			streamInterceptors,
			NewMetadataForwardingStreamClientInterceptor(headers))
	}

	dialOptions = append(
		dialOptions,
		grpc.WithChainUnaryInterceptor(unaryInterceptors...),
		grpc.WithChainStreamInterceptor(streamInterceptors...))
	return grpc.Dial(configuration.Address, dialOptions...)
}

// NewGRPCServersFromConfigurationAndServe creates a series of gRPC
// servers based on a configuration stored in a list of Protobuf
// messages. In then lets all of these gRPC servers listen on the
// network addresses of UNIX socket paths provided.
func NewGRPCServersFromConfigurationAndServe(configurations []*configuration.ServerConfiguration, registrationFunc func(*grpc.Server)) error {
	serveErrors := make(chan error)

	if len(configurations) == 0 {
		return status.Error(codes.InvalidArgument, "Expected GRPC server configuration is missing")
	}

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
				NewAuthenticatingUnaryInterceptor(authenticator)),
			grpc.ChainStreamInterceptor(
				grpc_prometheus.StreamServerInterceptor,
				NewAuthenticatingStreamInterceptor(authenticator)),
			grpc.StatsHandler(&ocgrpc.ServerHandler{}),
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

		// Optional: Keepalive enforcement policy.
		if policy := configuration.KeepaliveEnforcementPolicy; policy != nil {
			minTime, err := ptypes.Duration(policy.MinTime)
			if err != nil {
				return util.StatusWrap(err, "Failed to parse keepalive enforcement policy minimum time")
			}
			serverOptions = append(serverOptions, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             minTime,
				PermitWithoutStream: policy.PermitWithoutStream,
			}))
		}

		// Create server.
		s := grpc.NewServer(serverOptions...)
		registrationFunc(s)
		grpc_prometheus.Register(s)

		// Enable grpc reflection.
		reflection.Register(s)

		// Enable grpc health service
		h := health.NewServer()
		grpc_health_v1.RegisterHealthServer(s, h)
		// TODO: construct an API for the caller to indicate when it is healthy and set this. Until then, we'll just track this function's extry and exit.
		h.SetServingStatus(configuration.HealthCheckService, grpc_health_v1.HealthCheckResponse_SERVING)
		defer h.SetServingStatus(configuration.HealthCheckService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		if len(configuration.ListenAddresses)+len(configuration.ListenPaths) == 0 {
			return status.Error(codes.InvalidArgument, "GRPC server configured without any listen addresses or paths")
		}

		// TCP sockets.
		for _, listenAddress := range configuration.ListenAddresses {
			sock, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return util.StatusWrapf(err, "Failed to create listening socket for %#v", listenAddress)
			}
			go func() { serveErrors <- s.Serve(sock) }()
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
			go func() { serveErrors <- s.Serve(sock) }()
		}
	}
	return <-serveErrors
}
