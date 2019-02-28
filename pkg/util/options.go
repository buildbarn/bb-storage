package util

import (
	"bytes"
	"io/ioutil"
	"log"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func CreateServerOptions(tlsCertFile string, tlsKeyFile string, authMethod string, authSecretFile string) ([]grpc.ServerOption, error) {
	var options []grpc.ServerOption
	if tlsCertFile != "" && tlsKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(tlsCertFile, tlsKeyFile)
		if err != nil {
			return nil, err
		}
		options = append(options, grpc.Creds(creds))
	} else if tlsCertFile != "" || tlsKeyFile != "" {
		return nil, status.Errorf(codes.InvalidArgument, "Both certificate and key are required for TLS encryption")
	}

	si := []grpc.StreamServerInterceptor{grpc_prometheus.StreamServerInterceptor}
	ui := []grpc.UnaryServerInterceptor{grpc_prometheus.UnaryServerInterceptor}
	if authMethod == "jwt" {
		if tlsKeyFile == "" {
			log.Printf("Security warning: JWT authentication activated without TLS encryption!")
		}
		data, err := ioutil.ReadFile(authSecretFile)
		if err != nil {
			return nil, err
		}
		authCache := auth.NewJWTAuthCache(bytes.Trim(data, "\n"), 100)
		si = append(si, grpc_auth.StreamServerInterceptor(authCache.ValidateCredentials))
		ui = append(ui, grpc_auth.UnaryServerInterceptor(authCache.ValidateCredentials))
	} else if authMethod != "" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid authentication method: %#v", authMethod)
	}
	options = append(options, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(si...)))
	options = append(options, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(ui...)))
	return options, nil
}
