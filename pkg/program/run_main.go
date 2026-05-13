package program

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
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

// runMainConfig collects the optional behaviours of RunMain. Constructed
// internally; callers populate it via Option values.
type runMainConfig struct {
	daemon bool
}

// Option configures RunMain.
type Option func(*runMainConfig)

// WithDaemonExit causes a signal-triggered graceful shutdown to exit 0
// instead of 128+signal. Use this for long-running daemons (servers,
// workers, schedulers) where SIGINT/SIGTERM is the expected lifecycle
// event and a clean wind-down should look successful to the supervising
// process (k8s pod phase Succeeded, systemd inactive (dead) without
// failure).
//
// Without this option (the default), signal interruption exits with the
// POSIX-conventional 128+signal so wrapper scripts and init systems can
// distinguish a completed run from one that was interrupted mid-work.
// This is the right behaviour for one-shot CLI tools (bb_copy,
// sync_jwks_to_configmap, etc.).
func WithDaemonExit() Option {
	return func(c *runMainConfig) { c.daemon = true }
}

// terminateWithSignal completes a shutdown initiated by terminationSignal.
//
// Previously this re-raised the signal back to the process via
// signal.Reset() + process.Signal() so the container/init system would
// observe a signal-style exit (e.g. 128+SIGTERM=143). signal.Reset()
// does not install SIG_DFL though — the runtime's signal trampoline
// still catches the raised signal and dispatches to
// runtime.dieFromSignal(), which falls through to a hard exit(2) when
// its signal-to-self races. Multi-goroutine programs running as PID 1
// in a PID namespace reliably hit that fall-through, surfacing exit 2
// despite a clean shutdown.
//
// Skip the signal-raise dance and exit with the right code directly.
// 128+signal is what POSIX shells (bash, zsh) and init systems
// (systemd, kubelet) report from WIFSIGNALED anyway, so the
// user-visible exit is equivalent without going near Go's signal
// machinery. Daemons opt into exit 0 via WithDaemonExit so a graceful
// shutdown via SIGTERM does not look like a failure to the supervisor.
//
// Refs:
//   - https://github.com/golang/go/issues/19326
//   - https://github.com/golang/go/issues/46321
func terminateWithSignal(currentPID int, terminationSignal os.Signal, daemon bool) {
	if daemon {
		os.Exit(0)
	}
	if runtime.GOOS == "windows" {
		// On Windows, process.Signal() is not supported and
		// signal numbers do not map to POSIX exit codes; just
		// exit non-zero so wrapper scripts see the interruption.
		os.Exit(1)
	}
	if sig, ok := terminationSignal.(syscall.Signal); ok {
		os.Exit(128 + int(sig))
	}
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
//   - The program receives SIGINT or SIGTERM. By default the program
//     terminates with exit code 128+signal (the POSIX convention),
//     which is appropriate for one-shot tools. Pass WithDaemonExit to
//     exit 0 instead — appropriate for long-running daemons where a
//     signal-triggered shutdown is the normal lifecycle.
//
// In case termination occurs, all remaining routines are canceled,
// respecting dependencies between these routines. This can for example
// be used to ensure an outgoing database connection is terminated after
// an integrated RPC server is shut down.
func RunMain(routine Routine, opts ...Option) {
	var cfg runMainConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	currentPID := os.Getpid()
	relaunchIfPID1(currentPID, cfg.daemon)

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
			terminateWithSignal(currentPID, receivedSignal, cfg.daemon)
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
