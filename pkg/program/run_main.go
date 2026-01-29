package program

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// runMainErrorLogger is used by RunMain() to capture errors returned by
// goroutines. Each error is logged. Shutdown is initiated as soon as
// the first error arrives.
type runMainErrorLogger struct {
	shutdownStarted sync.Once
	shutdownFunc    func()
	cancel          context.CancelFunc
}

func (el *runMainErrorLogger) Log(err error) {
	log.Print("Fatal error: ", err)
	el.startShutdown(func() {
		os.Exit(1)
	})
}

func (el *runMainErrorLogger) startShutdown(shutdownFunc func()) {
	el.shutdownStarted.Do(func() {
		el.shutdownFunc = shutdownFunc
		el.cancel()
	})
}

// terminateWithSignal terminates the current process by sending a
// signal to itself.
func terminateWithSignal(currentPID int, terminationSignal os.Signal) {
	if runtime.GOOS == "windows" {
		// On Windows, process.Signal() is not supported so
		// immediately exit.
		os.Exit(1)
	}

	// Clear the signal handler and raise the
	// original signal once again. That way we shut
	// down under the original circumstances.
	signal.Reset(terminationSignal)
	process, err := os.FindProcess(currentPID)
	if err != nil {
		panic(err)
	}
	if err := process.Signal(terminationSignal); err != nil {
		panic(err)
	}

	// This code should not be reached, if it weren't for the fact
	// that process.Signal() does not guarantee that the signal is
	// delivered to the same thread.
	//
	// Furthermore, signal.Reset() does not reset signals that are
	// delivered via the process group, but ignored by the process
	// itself. Fall back to calling os.Exit() if we don't get
	// terminated via signal delivery.
	//
	// More details:
	// https://github.com/golang/go/issues/19326
	// https://github.com/golang/go/issues/46321
	time.Sleep(5)
	os.Exit(1)
}

var terminationSignals = []os.Signal{
	os.Interrupt,
	syscall.SIGTERM,
}

// RunMain runs a program that supports graceful termination. Programs
// consist of a pool of routines that may have dependencies on each
// other. Programs terminate if one of the following three cases occur:
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
func RunMain(routine Routine) {
	currentPID := os.Getpid()
	relaunchIfPID1(currentPID)

	ctx, cancel := context.WithCancel(context.Background())
	errorLogger := &runMainErrorLogger{
		cancel: cancel,
	}

	// Handle incoming signals.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, terminationSignals...)
	go func() {
		receivedSignal := <-signalChan
		log.Printf("Received %#v signal. Initiating graceful shutdown.", receivedSignal.String())
		errorLogger.startShutdown(func() {
			terminateWithSignal(currentPID, receivedSignal)
		})
	}()

	// Launch the initial routine and any goroutines that it spawns.
	run(ctx, errorLogger, routine)

	// If none of the routines failed and we didn't get signalled,
	// terminate with exit code zero.
	errorLogger.startShutdown(func() {
		os.Exit(0)
	})
	errorLogger.shutdownFunc()
}
