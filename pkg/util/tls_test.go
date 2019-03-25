package util

import (
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/server"
	"github.com/stretchr/testify/require"
)

func TestValidateGood_Files(t *testing.T) {
	cfg := &pb.TLSConfiguration{KeyFile: "testdata/key.pem", CertFile: "testdata/cert.pem"}
	require.Nil(t, ValidateTLS(cfg))
}

func TestValidateGood_FileAndInline(t *testing.T) {
	// actual values are not validated, just presence.
	cfg := &pb.TLSConfiguration{KeyFile: "somefile.pem", Cert: "cert contents"}
	require.Nil(t, ValidateTLS(cfg))
}

func TestValidateNotEnabled(t *testing.T) {
	require.Nil(t, ValidateTLS(nil))
}

func TestValidateBad_MissingKey(t *testing.T) {
	cfg := &pb.TLSConfiguration{KeyFile: "", CertFile: "testdata/cert.pem"}
	require.Equal(t, fmt.Errorf("Must supply both cert and key file or content if using TLS"),
		ValidateTLS(cfg))
}

func TestValidateBad_KeyBothFileAndInline(t *testing.T) {
	cfg := &pb.TLSConfiguration{
		CertFile: "somecert.pem",
		KeyFile:  "somefile.pem", Key: "key contents",
	}
	require.Equal(t, fmt.Errorf("Can only supply TLS key file OR contents, not both"),
		ValidateTLS(cfg))
}

func TestMakeGRPCCreds_TLS_Files(t *testing.T) {
	cfg := &pb.TLSConfiguration{KeyFile: "testdata/key.pem", CertFile: "testdata/cert.pem"}
	creds, err := MakeGRPCCreds(cfg)
	require.Nil(t, err)
	require.NotNil(t, creds)

	info := creds.Info()
	require.Equal(t, info.SecurityProtocol, "tls")
	require.Equal(t, info.SecurityVersion, "1.2")
}

func TestMakeGRPCCreds_TLS_CertInline(t *testing.T) {
	cfg := &pb.TLSConfiguration{
		KeyFile: "testdata/key.pem",
		Cert: `-----BEGIN CERTIFICATE-----
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
Qyelh/2wseRhE4iuh5cb03zBli0uwxIXbIE=
-----END CERTIFICATE-----`,
	}
	creds, err := MakeGRPCCreds(cfg)
	require.Nil(t, err)
	require.NotNil(t, creds)

	info := creds.Info()
	require.Equal(t, info.SecurityProtocol, "tls")
	require.Equal(t, info.SecurityVersion, "1.2")
}

func TestMakeGRPCCreds_NoTLS(t *testing.T) {
	creds, err := MakeGRPCCreds(nil)
	require.Nil(t, err)
	require.Nil(t, creds)
}

func TestMakeGRPCCreds_TLS_FileMissing(t *testing.T) {
	cert, key := "testdata/cert.pem", "testdata/keyNOT.pem"
	cfg := &pb.TLSConfiguration{KeyFile: key, CertFile: cert}
	creds, err := MakeGRPCCreds(cfg)
	require.Nil(t, creds)
	require.Equal(t,
		"Can't load X509 certificate and keypair with config key_file:\"testdata/keyNOT.pem\" cert_file:\"testdata/cert.pem\" , caused by open testdata/keyNOT.pem: no such file or directory",
		err.Error())
}

func TestStartHttp_NoTLS(t *testing.T) {
	successfulHTTPStart(t, nil)
}

func TestStartHttp_WithTLS_Files(t *testing.T) {
	successfulHTTPStart(t, &pb.TLSConfiguration{
		CertFile: "testdata/cert.pem", KeyFile: "testdata/key.pem"})
}

func TestStartHttp_WithTLS_CertInline(t *testing.T) {
	successfulHTTPStart(t, &pb.TLSConfiguration{
		KeyFile: "testdata/key.pem",
		Cert: `-----BEGIN CERTIFICATE-----
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
Qyelh/2wseRhE4iuh5cb03zBli0uwxIXbIE=
-----END CERTIFICATE-----
`,
	})
}

func successfulHTTPStart(t *testing.T, cfg *pb.TLSConfiguration) {
	addr := ":0"
	srv, listener, listenErr := makeHTTPServer(&addr, cfg, nil)
	require.Nil(t, listenErr)
	defer listener.Close()

	go func() {
		serveErr := httpServe(srv, listener)
		t.Logf("serve failed or completed with: %v", serveErr)

	}()
	defer srv.Close()

	time.Sleep(1 * time.Second)

	// check the server is actually in good shape
	withTLS := cfg != nil
	var protocol string
	if withTLS {
		protocol = "https"
	} else {
		protocol = "http"
	}
	actualAddr := listener.Addr().String()
	_, getErr := http.Get(protocol + "://" + actualAddr + "/")
	if !withTLS {
		require.Nil(t, getErr)
	} else {
		// will not be fully successful with the self-published certificate,
		// so success is a specific X509 error.
		switch errSub := getErr.(type) {
		case *url.Error:
			switch errSub.Err.(type) {
			case x509.HostnameError:
				// as good as it gets without a proper certificate for this host.
			default:
				t.Errorf("TLS URL error had unexpected cause, was %v", getErr)
			}
		default:
			t.Errorf("Unexpected http-TLS request error %v", getErr)
		}
	}
}
