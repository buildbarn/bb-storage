package program

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
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

	shutdownStarted sync.Once
	shutdownFunc    func()
	cancel          context.CancelFunc
}

func (root *groupsRoot) startShutdown(shutdownFunc func()) {
	root.shutdownStarted.Do(func() {
		root.shutdownFunc = shutdownFunc
		root.cancel()
	})
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
		// Some error occurred. Initiate cancelation of the
		// entire program with a non-zero exit code.
		log.Print("Fatal error: ", err)
		sg.root.startShutdown(func() {
			os.Exit(1)
		})
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

// Run a program that supports graceful termination. Programs consist of
// a pool of routines that may have dependencies on each other. Programs
// terminate if one of the following three cases occur:
//
//   - The root routine and all of its siblings have terminated. In that
//     case the program terminates with exit code 0.
//
//   - One of the routines fails with a non-nil error. In that case the
//     program terminates with exit code 1.
//
//   - The program receives SIGINT or SIGTERM. In that case the program
//     will terminate with that signal.
//
// In case termination occurs, all remaining routines are canceled,
// respecting dependencies between these routines. This can for example
// be used to ensure an outgoing database connection is terminated after
// an integrated RPC server is shut down.
func Run(routine Routine) {
	// Install the signal handler. This needs to be done first to
	// ensure no signals are missed.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Launch the initial routine.
	ctx, cancel := context.WithCancel(context.Background())
	root := groupsRoot{
		cancel: cancel,
	}
	sg := newSiblingsGroup(ctx, &root)
	go sg.runRoutine(routine)

	// Handle incoming signals.
	go func() {
		receivedSignal := <-signalChan
		log.Printf("Received %#v signal. Initiating graceful shutdown.", receivedSignal.String())
		root.startShutdown(func() {
			// Clear the signal handler and raise the
			// original signal once again. That way we shut
			// down under the original circumstances.
			signal.Reset(receivedSignal)
			process, err := os.FindProcess(os.Getpid())
			if err != nil {
				panic(err)
			}
			if err := process.Signal(receivedSignal); err != nil {
				panic(err)
			}
			// This code should not be reached, if it
			// weren't for the fact that process.Signal()
			// does not guarantee that the signal is
			// delivered to the same thread. More details:
			// https://github.com/golang/go/issues/19326
			select {}
		})
	}()

	// Wait for all of the routines in the program to complete.
	root.siblingsGroupsCount.Wait()

	// If none of the routines failed and we didn't get signalled,
	// terminate with exit code zero.
	root.startShutdown(func() {
		os.Exit(0)
	})
	root.shutdownFunc()
}
