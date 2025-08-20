package blockdevice

import (
	"context"

	"golang.org/x/sync/semaphore"
)

type writeConcurrencyLimitingBlockDevice struct {
	BlockDevice
	semaphore *semaphore.Weighted
}

// NewWriteConcurrencyLimitingBlockDevice is a decorator for BlockDevice
// that limits the number of calls to WriteAt() that may run in
// parallel. This can be used to prevent exhaustion of operating system
// level threads, which can cause the Go runtime to crash the process.
func NewWriteConcurrencyLimitingBlockDevice(base BlockDevice, semaphore *semaphore.Weighted) BlockDevice {
	return &writeConcurrencyLimitingBlockDevice{
		BlockDevice: base,
		semaphore:   semaphore,
	}
}

func (bd *writeConcurrencyLimitingBlockDevice) WriteAt(p []byte, off int64) (int, error) {
	if err := bd.semaphore.Acquire(context.Background(), 1); err != nil {
		panic("acquiring semaphore with background context should never fail")
	}
	defer bd.semaphore.Release(1)

	return bd.BlockDevice.WriteAt(p, off)
}
