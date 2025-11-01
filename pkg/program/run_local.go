package program

import (
	"context"
	"sync"
)

type runLocalErrorLogger struct {
	shutdownStarted sync.Once
	firstError      error
	cancel          context.CancelFunc
}

func (el *runLocalErrorLogger) Log(err error) {
	el.shutdownStarted.Do(func() {
		el.firstError = err
		el.cancel()
	})
}

// RunLocal runs a set of goroutines until completion. This function
// provides the same functionality as errgroup.Group, with a couple of
// differences:
//
//   - There is no need to call a separate Wait() function. This
//     prevents leaking Context objects by accident.
//
//   - Like RunMain(), routines are placed in a hierarchy of siblings
//     and dependencies, making it easier to trigger shutdown.
func RunLocal(ctx context.Context, routine Routine) error {
	innerCtx, cancel := context.WithCancel(ctx)
	errorLogger := &runLocalErrorLogger{
		cancel: cancel,
	}
	run(innerCtx, errorLogger, routine)
	errorLogger.shutdownStarted.Do(cancel)
	return errorLogger.firstError
}
