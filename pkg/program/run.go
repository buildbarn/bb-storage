package program

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// Routine that can be executed as part of a program. Routines may
// include web/gRPC servers, storage flushing processes or clients that
// repeatedly perform requests against a remote service.
//
// Each routine is capable of launching additional routines that either
// run as siblings, or as dependencies of the current routine and
// its siblings. Siblings are all terminated at the same time, while
// dependencies are only terminated after all of the siblings of the
// current routine have completed.
type Routine func(ctx context.Context, siblingsGroup, dependenciesGroup Group) error

// Group of routines. This interface can be used to launch additional
// routines.
type Group interface {
	Go(routine Routine)
}

// groupsRoot contains bookkeeping that is shared across all groups
// within the current program.
type groupsRoot struct {
	siblingsGroupsCount sync.WaitGroup
	errorLogger         util.ErrorLogger
}

// siblingsGroup is a group of routines that are all siblings with
// respect to each other.
type siblingsGroup struct {
	root                *groupsRoot
	siblingsActive      atomic.Uint32
	siblingsContext     context.Context
	dependenciesContext context.Context
	dependenciesCancel  context.CancelFunc
}

// newSiblingsGroup constructs a new siblingsGroup that contains exactly
// one routine. The caller MUST call runRoutine() on it after creation
// to actually start execution of this routine.
func newSiblingsGroup(siblingsContext context.Context, root *groupsRoot) *siblingsGroup {
	dependenciesContext, dependenciesCancel := context.WithCancel(context.Background())
	sg := &siblingsGroup{
		root:                root,
		siblingsContext:     siblingsContext,
		dependenciesContext: dependenciesContext,
		dependenciesCancel:  dependenciesCancel,
	}
	sg.siblingsActive.Store(1)
	root.siblingsGroupsCount.Add(1)
	return sg
}

func (sg *siblingsGroup) runRoutine(routine Routine) {
	if err := routine(
		sg.siblingsContext,
		sg,
		dependenciesGroup{siblingsGroup: sg},
	); err != nil {
		sg.root.errorLogger.Log(err)
	}

	if sg.siblingsActive.Add(^uint32(0)) == 0 {
		// This is the last sibling that terminated. We can now
		// safely terminate our dependencies.
		sg.dependenciesCancel()
		sg.root.siblingsGroupsCount.Done()
	}
}

func (sg *siblingsGroup) Go(routine Routine) {
	if sg.siblingsActive.Add(1) < 2 {
		panic("Attempted to create a goroutine in a group that is already completed")
	}
	go sg.runRoutine(routine)
}

type dependenciesGroup struct {
	siblingsGroup *siblingsGroup
}

func (dg dependenciesGroup) Go(routine Routine) {
	sg := dg.siblingsGroup
	if sg.siblingsActive.Load() == 0 {
		panic("Attempted to create a goroutine in a group that is already completed")
	}

	// Create a new siblings group, so that this newly spawned
	// routine can also have its own set of siblings.
	childSG := newSiblingsGroup(sg.dependenciesContext, sg.root)
	go childSG.runRoutine(routine)
}

func run(ctx context.Context, errorLogger util.ErrorLogger, routine Routine) {
	root := groupsRoot{
		errorLogger: errorLogger,
	}
	sg := newSiblingsGroup(ctx, &root)
	sg.runRoutine(routine)
	root.siblingsGroupsCount.Wait()
}
