package program

import (
	"context"
)

// Startup makes a blocking call to f to populate group with
// siblings and dependencies.
func Startup[T any](group Group, f func(ctx context.Context, siblingsGroup, dependenciesGroup Group) T) T {
	resultChan := make(chan T, 1)
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup Group) error {
		resultChan <- f(ctx, siblingsGroup, dependenciesGroup)
		return nil
	})
	return <-resultChan
}

// StartupSiblings makes a blocking call to f to populate group with
// siblings.
func StartupSiblings[T any](group Group, f func(ctx context.Context, terminationGroup Group) T) T {
	return Startup(group, func(ctx context.Context, siblingsGroup, dependenciesGroup Group) T {
		return f(ctx, siblingsGroup)
	})
}

// StartupDependencies makes a blocking call to f to populate group with
// dependencies. The context given to f is the context of the
// dependencies group.
func StartupDependencies[T any](group Group, f func(ctx context.Context, terminationGroup Group) T) T {
	return Startup(group, func(ctx context.Context, siblingsGroup, dependenciesGroup Group) T {
		return StartupSiblings(dependenciesGroup, f)
	})
}
