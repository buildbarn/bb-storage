package grpc

import (
	"context"
	"net"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/trace"
)

func init() {
	// Add Prometheus timing metrics.
	grpc_prometheus.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets(
			util.DecimalExponentialBuckets(-3, 6, 2)))
}

// ServerRMDHandler is meant to wrap ocgrpc.ServerHandler, and exports additional information
// about the RPC from REAPI request metadata
type ServerRMDHandler struct {
	stats.Handler
}

// TagRPC adds REAPI request metadata to an existing OpenCensus span.
func (ssh *ServerRMDHandler) TagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	ctx = ssh.Handler.TagRPC(ctx, rti)

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	rmds := md.Get("build.bazel.remote.execution.v2.requestmetadata-bin")
	if len(rmds) == 0 {
		return ctx
	}

	rmd := &remoteexecution.RequestMetadata{}
	err := proto.Unmarshal([]byte(rmds[0]), rmd)
	if err != nil {
		return ctx
	}

	span := trace.FromContext(ctx)
	if span == nil {
		return ctx
	}
	span.AddAttributes(
		trace.StringAttribute("invocation_id", rmd.ToolInvocationId),
	)
	return ctx
}

// NewServersFromConfigurationAndServe creates a series of gRPC servers
// based on a configuration stored in a list of Protobuf messages. It
// then lets all of these gRPC servers listen on the network addresses
// of UNIX socket paths provided.
func NewServersFromConfigurationAndServe(configurations []*configuration.ServerConfiguration, registrationFunc func(*grpc.Server)) error {
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
			grpc.StatsHandler(&ServerRMDHandler{&ocgrpc.ServerHandler{}}),
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
