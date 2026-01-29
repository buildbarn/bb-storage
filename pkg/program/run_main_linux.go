//go:build linux

package program

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// relaunchIfPID1 relaunches the executable if the current process has
// PID 1, and propagates its termination status.
//
// Go programs typically only call syscall.Wait4() against individual
// PIDs. This is fine for ordinary processes. However, when a program
// runs as PID 1, other processes may be reparented to it. In this case
// we should be calling syscall.Wait4() with the PID set to -1.
//
// Because it is unsafe to call syscall.Wait4() with the PID set to -1
// while parts of the Go standard library wait for individual processes
// to terminate, we need to run multiple processes.
//
// More details: https://github.com/golang/go/pull/61261
func relaunchIfPID1(currentPID int) {
	if currentPID == 1 {
		executable, err := os.Executable()
		if err != nil {
			log.Fatal("Failed to obtain path of current executable: ", err)
		}

		signal.Ignore(terminationSignals...)
		childPID, _, err := syscall.StartProcess(executable, os.Args, &syscall.ProcAttr{
			Env:   os.Environ(),
			Files: []uintptr{0, 1, 2},
		})
		if err != nil {
			log.Fatal("Failed to relaunch current process: ", err)
		}

		for {
			var status syscall.WaitStatus
			waitedPID, err := syscall.Wait4(-1, &status, 0, nil)
			for err == syscall.EINTR {
				waitedPID, err = syscall.Wait4(-1, &status, 0, nil)
			}
			if err != nil {
				log.Fatal("Failed to wait for process termination: ", err)
			}

			if waitedPID == childPID {
				if status.Signaled() {
					terminateWithSignal(currentPID, status.Signal())
				}
				os.Exit(status.ExitStatus())
			}
		}
	}
}
