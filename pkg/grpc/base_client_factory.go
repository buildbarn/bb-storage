package grpc

import (
	"context"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"go.opencensus.io/plugin/ocgrpc"
)

func init() {
	// Add Prometheus timing metrics.
	grpc_prometheus.EnableClientHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets(
			util.DecimalExponentialBuckets(-3, 6, 2)))
}

type baseClientFactory struct{}

func (cf baseClientFactory) NewClientFromConfiguration(config *configuration.ClientConfiguration) (grpc.ClientConnInterface, error) {
	if config == nil {
		return nil, status.Error(codes.InvalidArgument, "No gRPC client configuration provided")
	}

	dialOptions := []grpc.DialOption{
		grpc.WithStatsHandler(&ocgrpc.ClientHandler{}),
	}
	unaryInterceptors := []grpc.UnaryClientInterceptor{
		grpc_prometheus.UnaryClientInterceptor,
	}
	streamInterceptors := []grpc.StreamClientInterceptor{
		grpc_prometheus.StreamClientInterceptor,
	}

	// Optional: TLS.
	tlsConfig, err := util.NewTLSConfigFromClientConfiguration(config.Tls)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create TLS configuration")
	}
	if tlsConfig != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	// Optional: OAuth authentication.
	if oauthConfig := config.Oauth; oauthConfig != nil {
		var perRPC credentials.PerRPCCredentials
		var err error
		switch credentials := oauthConfig.Credentials.(type) {
		case *configuration.ClientOAuthConfiguration_GoogleDefaultCredentials:
			perRPC, err = oauth.NewApplicationDefault(context.Background(), oauthConfig.Scopes...)
		case *configuration.ClientOAuthConfiguration_ServiceAccountKey:
			perRPC, err = oauth.NewServiceAccountFromKey([]byte(credentials.ServiceAccountKey), oauthConfig.Scopes...)
		default:
			return nil, status.Error(codes.InvalidArgument, "gRPC client credentials are wrong: one of googleDefaultCredentials or serviceAccountKey should be provided")
		}
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create gRPC credentials")
		}
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(perRPC))
	}

	// Optional: Keepalive.
	if config.Keepalive != nil {
		time, err := ptypes.Duration(config.Keepalive.Time)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse keepalive time")
		}
		timeout, err := ptypes.Duration(config.Keepalive.Timeout)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse keepalive timeout")
		}
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time,
			Timeout:             timeout,
			PermitWithoutStream: config.Keepalive.PermitWithoutStream,
		}))
	}

	// Optional: metadata forwarding.
	if headers := config.ForwardMetadata; len(headers) > 0 {
		unaryInterceptors = append(
			unaryInterceptors,
			NewMetadataForwardingUnaryClientInterceptor(headers))
		streamInterceptors = append(
			streamInterceptors,
			NewMetadataForwardingStreamClientInterceptor(headers))
	}

	// Optional: add metadata.
	if headers := config.AddMetadata; len(headers) > 0 {
		var headerValues MetadataHeaderValues
		for _, entry := range headers {
			headerValues.Add(entry.Header, entry.Values)
		}
		unaryInterceptors = append(
			unaryInterceptors,
			NewMetadataAddingUnaryClientInterceptor(headerValues))
		streamInterceptors = append(
			streamInterceptors,
			NewMetadataAddingStreamClientInterceptor(headerValues))
	}

	// Optional: metadata forwarding with reuse.
	if headers := config.ForwardAndReuseMetadata; len(headers) > 0 {
		interceptor := NewMetadataForwardingAndReusingInterceptor(headers)
		unaryInterceptors = append(unaryInterceptors, interceptor.InterceptUnaryClient)
		streamInterceptors = append(streamInterceptors, interceptor.InterceptStreamClient)
	}

	dialOptions = append(
		dialOptions,
		grpc.WithChainUnaryInterceptor(unaryInterceptors...),
		grpc.WithChainStreamInterceptor(streamInterceptors...))
	return grpc.Dial(config.Address, dialOptions...)
}

// BaseClientFactory creates gRPC clients using the go-grpc library.
var BaseClientFactory ClientFactory = baseClientFactory{}
