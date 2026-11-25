package program_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/stretchr/testify/require"
)

func TestProgramGroup(t *testing.T) {
	t.Run("DependenciesCancelledAfterSiblingRoutinesFinish", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var sibContext context.Context
			var depContext context.Context
			sibRelease := make(chan struct{})
			depRelease := make(chan struct{})

			go func() {
				synctest.Wait()
				require.NoError(t, sibContext.Err())
				require.NoError(t, depContext.Err())

				close(sibRelease)
				// TODO: sibContext should be cancelled when the siblings finished.
				// <-sibContext.Done()
				<-depContext.Done()

				close(depRelease)
			}()

			program.RunLocal(t.Context(), func(ctx context.Context, outerSiblingsGroup, outerDependenciesGroup program.Group) error {
				outerSiblingsGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					sibContext = ctx
					<-sibRelease
					return nil
				})
				outerDependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					depContext = ctx
					<-depRelease
					return nil
				})
				return nil
			})
		})
	})

	t.Run("CancellingContextOnlyAffectsSiblings", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			var sibContext context.Context
			var depContext context.Context
			sibRelease := make(chan struct{})
			depRelease := make(chan struct{})

			go func() {
				synctest.Wait()
				require.NoError(t, sibContext.Err())
				require.NoError(t, depContext.Err())

				cancel()
				<-sibContext.Done()
				synctest.Wait()
				require.NoError(t, depContext.Err())

				close(sibRelease)
				<-depContext.Done()
				close(depRelease)
			}()

			program.RunLocal(ctx, func(ctx context.Context, outerSiblingsGroup, outerDependenciesGroup program.Group) error {
				outerSiblingsGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					sibContext = ctx
					<-sibRelease
					return nil
				})
				outerDependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					depContext = ctx
					<-depRelease
					return nil
				})
				return nil
			})
		})
	})
}
