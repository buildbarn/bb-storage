package global

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// InstallGracefulTerminationHandler installs signal handlers, so that
// this process gets notified when it is requested to shut down.
//
// This method returns a Context that the caller can use to detect that
// shutdown is initiated. It also returns a WaitGroup that the caller
// can use to schedule tasks that must complete prior to shutting down.
// Goroutines scheduled in this WaitGroup must respect cancellation of
// the Context.
func InstallGracefulTerminationHandler() (context.Context, *sync.WaitGroup) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	var group sync.WaitGroup

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		receivedSignal := <-signalChan
		log.Printf("Received %s signal. Initiating graceful shutdown.", receivedSignal.String())

		// Inform other parts of the process about the impending
		// shutdown. Wait for cleanup/shutdown tasks to complete.
		ctxCancel()
		group.Wait()
		log.Print("Graceful shutdown complete")

		// Clear the signal handler and raise the original
		// signal once again. That way we shut down under the
		// original circumstances.
		signal.Reset(receivedSignal)
		process, err := os.FindProcess(os.Getpid())
		if err != nil {
			panic(err)
		}
		if err := process.Signal(receivedSignal); err != nil {
			panic(err)
		}
		panic("Raising the original signal didn't cause us to shut down")
	}()

	return ctx, &group
}
