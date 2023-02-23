package http

import (
	"context"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// LaunchServer spawns a HTTP server as part of a program.Group. The web
// server is automatically terminated if the context associated with the
// group is canceled.
func LaunchServer(server *http.Server, group program.Group) {
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
