package spire

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"io/fs"
	"os"
)

const SPIFFE_ENDPT = "SPIFFE_ENDPOINT_SOCKET"

func HasSpireEndpoint() bool {
	s := os.Getenv(SPIFFE_ENDPT)
	if s == "" {
		return false
	}
	fi, err := os.Stat(s)
	if err != nil {
		return false
	}
	if fi.Mode().Type()&fs.ModeSocket != 0 {
		return true
	}
	return false
}

func GetTlsClientConfig() (*tls.Config, error) {
	s := os.Getenv(SPIFFE_ENDPT)
	if s == "" {
		return nil, fmt.Errorf(SPIFFE_ENDPT + " not found in environment")
	}
	src, err := workloadapi.NewX509Source(context.Background(), workloadapi.WithClientOptions(workloadapi.WithAddr("unix://"+s)))
	if err != nil {
		return nil, err
	}
	defer src.Close()

	tlsConfig := tlsconfig.MTLSClientConfig(src, src, tlsconfig.AuthorizeAny())
	return tlsConfig, nil
}

func GetTlsServerConfig() (*tls.Config, error) {
	s := os.Getenv(SPIFFE_ENDPT)
	if s == "" {
		return nil, fmt.Errorf(SPIFFE_ENDPT + " not found in environment")
	}
	src, err := workloadapi.NewX509Source(context.Background(), workloadapi.WithClientOptions(workloadapi.WithAddr("unix://"+s)))
	if err != nil {
		return nil, err
	}
	defer src.Close()

	tlsConfig := tlsconfig.MTLSServerConfig(src, src, tlsconfig.AuthorizeAny())
	return tlsConfig, nil
}
