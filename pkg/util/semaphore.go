package util

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// AcquireSemaphore acquires a weighted semaphore.
//
// Weighted.Acquire() does not check for context cancellation prior to
// acquiring. This means that if the semaphore is acquired in a tight
// loop, the loop will not be interrupted. This helper function
// rectifies that.
func AcquireSemaphore(ctx context.Context, semaphore *semaphore.Weighted, count int) error {
	if ctx.Err() != nil || semaphore.Acquire(ctx, 1) != nil {
		return StatusFromContext(ctx)
	}
	return nil
}
