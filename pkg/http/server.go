package http

import (
	"context"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/http"
	"github.com/buildbarn/bb-storage/pkg/util"
)

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
			for _, listenAddress := range configuration.ListenAddresses {
				server := http.Server{
					Addr:    listenAddress,
					Handler: authenticatedHandler,
				}
				group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					<-ctx.Done()
					return server.Close()
				})
				group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					if err := server.ListenAndServe(); err != http.ErrServerClosed {
						return util.StatusWrapf(err, "Failed to launch HTTP server %#v", server.Addr)
					}
					return nil
				})
			}
		}
		return nil
	})
}
