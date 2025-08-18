package clock

import (
	"context"
	"time"
)

// Clock is an interface around some of the standard library functions
// that provide time handling. It has been added to aid unit testing.
type Clock interface {
	// Return the current time of day. Equivalent to time.Now().
	Now() time.Time

	// Create a Context object that automatically cancels after a
	// certain amount of time has passed. Equivalent to
	// context.WithTimeout().
	NewContextWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc)

	// Create a channel that publishes the time of day at a point of
	// time in the future. Unlike time.NewTimer(), this function
	// returns the channel directly to allow Timer to be an
	// interface.
	NewTimer(d time.Duration) (Timer, <-chan time.Time)

	// Create a channel that will publish the time of day at a regular
	// interval.
	NewTicker(d time.Duration) (Ticker, <-chan time.Time)
}

// Timer is an interface around time.Timer. It has been added to aid
// unit testing.
type Timer interface {
	Stop() bool
}

// Ticker is an interface around time.Ticker. It has been added to aid
// unit testing.
type Ticker interface {
	Stop()
}
