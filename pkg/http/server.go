package http

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/bb_tls"
	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/http"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type certInfo struct {
	mu         sync.Mutex
	x509Certs  []*x509.Certificate
	privateKey crypto.Signer
}

// NewServersFromConfigurationAndServe spawns HTTP servers as part of a
// program.Group, based on a configuration message. The web servers are
// automatically terminated if the context associated with the group is
// canceled.
func NewServersFromConfigurationAndServe(configurations []*configuration.ServerConfiguration, handler http.Handler, group program.Group) {
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		for _, configuration := range configurations {
			authenticator, err := NewAuthenticatorFromConfiguration(configuration.AuthenticationPolicy, dependenciesGroup)
			if err != nil {
				return err
			}
			authenticatedHandler := NewAuthenticatingHandler(handler, authenticator)
			var cfg *tls.Config
			var certPath, keyPath string
			if configuration.Tls != nil {
				var ci certInfo
				pair := configuration.Tls.GetServerKeyPair()
				if pair == nil {
					return fmt.Errorf("HTTPS TLS configuration requires a server certificate/key pair")
				}
				files := pair.GetFiles()
				if files != nil {
					certPath = files.GetCertificatePath()
					keyPath = files.GetPrivateKeyPath()
					if !util.IsPEMFile(certPath) {
						return fmt.Errorf("HTTPS TLS server certificate must be stored in a PEM file")
					}
					if !util.IsPEMFile(keyPath) {
						return fmt.Errorf("HTTPS TLS server private key must be stored in a PEM file")
					}
					cfg, err := bb_tls.GetBaseTLSConfig(configuration.Tls.CipherSuites)
					if err != nil {
						return err
					}
					cfg.ClientAuth = tls.NoClientCert
					ci.mu.Lock()
					err = ci.loadNewCerts(certPath, keyPath)
					ci.mu.Unlock()
					if err != nil {
						log.Fatal(err.Error())
					}
					cfg.GetCertificate = ci.getCertificate(certPath, keyPath)
				} else {
					inline := pair.GetInline()
					if inline != nil {
						cfg, err := bb_tls.GetBaseTLSConfig(configuration.Tls.CipherSuites)
						if err != nil {
							return err
						}
						cfg.ClientAuth = tls.NoClientCert
						cert, err := tls.X509KeyPair([]byte(inline.GetCertificate()), []byte(inline.GetPrivateKey()))
						if err != nil {
							log.Fatal("Invalid server certificate or private key: %v", err)
						}
						cfg.Certificates = []tls.Certificate{cert}
					}
				}
			}
			for _, listenAddress := range configuration.ListenAddresses {
				server := http.Server{
					Addr:      listenAddress,
					Handler:   authenticatedHandler,
					TLSConfig: cfg,
				}
				group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					<-ctx.Done()
					return server.Close()
				})
				group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					var err error
					if configuration.Tls != nil {
						l, err := tls.Listen("tcp", listenAddress, cfg)
						if err != nil {
							log.Fatal("can't listen: %v", err)
						}
						// For some reason, ListenAndServeTLS isn't picking up the GetCertificate function and
						// requires file paths, which just happen to not be reloaded when the certs expire.
						// err = server.ListenAndServeTLS(certPath, keyPath)
						err = server.Serve(l)
					} else {
						err = server.ListenAndServe()
					}
					if err != http.ErrServerClosed {
						return util.StatusWrapf(err, "Failed to launch HTTP server %#v", server.Addr)
					}
					return nil
				})
			}
		}
		return nil
	})
}

func (ci *certInfo) loadNewCerts(certFile, keyFile string) error {
	cb, err := ioutil.ReadFile(certFile)
	if err != nil {
		return fmt.Errorf("can't read certs: %v", err)
	}
	kb, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return fmt.Errorf("can't read key: %v", err)
	}
	svid, err := x509svid.Parse(cb, kb)
	if err != nil {
		return fmt.Errorf("can't parse certs/key: %v", err)
	}
	ci.x509Certs = svid.Certificates
	ci.privateKey = svid.PrivateKey
	return nil
}

func (ci *certInfo) getCertificate(certFile, keyFile string) func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
		ci.mu.Lock()
		defer ci.mu.Unlock()
		log.Printf("ClientHelloInfo: %#v\n", info)
		log.Printf("HTTPS CI: getCert not before %v not after %v\n", ci.x509Certs[0].NotBefore, ci.x509Certs[0].NotAfter)
		if time.Now().After(ci.x509Certs[0].NotAfter.Add(time.Minute * -15)) {
			// Cert is about to expire.  Some external entity is responsible for rotating certs.
			// Reload the new ones.
			if err := ci.loadNewCerts(certFile, keyFile); err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "Can't reload certs: %v\n", err)
			}
			log.Printf("HTTPS CI: Reload: getCert not before %v not after %v\n", ci.x509Certs[0].NotBefore, ci.x509Certs[0].NotAfter)
		}
		cert := &tls.Certificate {
			Certificate: make([][]byte, 0, len(ci.x509Certs)),
			PrivateKey:  ci.privateKey,
		}
		for _, c := range ci.x509Certs {
			cert.Certificate = append(cert.Certificate, c.Raw)
		}
		return cert, nil
	}
}
