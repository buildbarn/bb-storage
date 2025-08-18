package clock

import (
	"context"
	"time"
)

type systemClock struct{}

func (c systemClock) Now() time.Time {
	return time.Now()
}

func (c systemClock) NewContextWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

func (c systemClock) NewTimer(d time.Duration) (Timer, <-chan time.Time) {
	t := time.NewTimer(d)
	return t, t.C
}

func (c systemClock) NewTicker(d time.Duration) (Ticker, <-chan time.Time) {
	t := time.NewTicker(d)
	return t, t.C
}

// SystemClock is a Clock that corresponds to the current time of day,
// as reported by the operating system.
var SystemClock Clock = systemClock{}
