package util

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/server"
	"google.golang.org/grpc/credentials"
)

// ValidateTLS checks the configuration is in order and returns non-nil if not.
// must be called before using the configuration.
func ValidateTLS(p *pb.TLSConfiguration) error {
	if p == nil {
		return nil
	}
	haveCert := p.CertFile != "" || p.Cert != ""
	haveKey := p.KeyFile != "" || p.Key != ""
	if !(haveCert && haveKey) {
		return errors.New("Must supply both cert and key file or content if using TLS")
	}
	if p.CertFile != "" && p.Cert != "" {
		return errors.New("Can only supply TLS certificate file OR contents, not both")
	}
	if p.KeyFile != "" && p.Key != "" {
		return errors.New("Can only supply TLS key file OR contents, not both")
	}

	return nil
}

// MakeGRPCCreds Creates TLS credentials for GRPC.
// returns nil,nil to indicate no TLS if it's not configured.
func MakeGRPCCreds(tlsParams *pb.TLSConfiguration) (credentials.TransportCredentials, error) {
	if tlsParams == nil {
		return nil, nil
	}
	cfg, err := makeTLSConfig(tlsParams)
	if err != nil {
		return nil,
			fmt.Errorf("Can't load X509 certificate and keypair with config %v, caused by %v",
				tlsParams, err)
	}
	creds := credentials.NewTLS(cfg)
	return creds, nil
}

// makeTLSConfig Creates a tls.Config instance based on bb-storage config file TLS snippet.
// returns nil,nil to indicate no TLS if no config is passed in.
func makeTLSConfig(tlsParams *pb.TLSConfiguration) (*tls.Config, error) {
	if tlsParams == nil {
		return nil, nil
	}
	keyBytes, err := loadIfFile(tlsParams.KeyFile, []byte(tlsParams.Key))
	if err != nil {
		return nil, err
	}
	certBytes, err := loadIfFile(tlsParams.CertFile, []byte(tlsParams.Cert))
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("Can't load X509 certificate and keypair with config %v, caused by %v",
			tlsParams, err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	return cfg, nil
}

// after validation, only one of these args will be non-empty
func loadIfFile(filename string, contents []byte) ([]byte, error) {
	if filename != "" {
		bytes, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		return bytes, nil
	}
	return contents, nil
}

func makeHTTPServer(addr *string, tlsParams *pb.TLSConfiguration, handler http.Handler) (*http.Server, net.Listener, error) {
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		return nil, nil, err
	}
	var cfg *tls.Config
	if tlsParams != nil {
		if valErr := ValidateTLS(tlsParams); valErr != nil {
			return nil, nil, valErr
		}
		cfg, err = makeTLSConfig(tlsParams)
		if err != nil {
			return nil, nil, err
		}
	}
	s := &http.Server{
		Addr:      *addr,
		Handler:   handler,
		TLSConfig: cfg,
	}
	return s, l, nil
}

func httpServe(server *http.Server, listener net.Listener) error {
	var err error
	if server.TLSConfig == nil {
		err = server.Serve(listener)
	} else {
		// we already set server.TLSConfig, so no more TLS materials here.
		err = server.ServeTLS(listener, "", "")
	}
	return err
}

// HTTPListenAndServe starts the http(s) server, using TLS if
// TLS materials were specified (non-nil)
func HTTPListenAndServe(addr *string, cfg *pb.TLSConfiguration, handler http.Handler) error {
	server, listener, err := makeHTTPServer(addr, cfg, handler)
	if err != nil {
		return fmt.Errorf("Failed to listen on %s, caused by %v", *addr, err)
	}
	defer listener.Close()
	return httpServe(server, listener)
}
