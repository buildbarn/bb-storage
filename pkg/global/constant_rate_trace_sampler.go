package global

import (
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"

	"go.opencensus.io/trace"
)

// ConstantRateTraceSampler uses a token bucket algorithm to sample traces. This is useful as an alternative
// to probability-based sampling when the service has a low number of requests per second.
type ConstantRateTraceSampler struct {
	clock               clock.Clock
	maxTokens           int64
	periodDurationNanos int64
	tokensPerPeriod     int64
	tokensPerSample     int64

	lock            sync.Mutex
	availableTokens int64
	lastRefillTime  time.Time
	nextRefillTime  time.Time
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Sample decides whether to sample a trace. The trace.Sampler type is unfortunately a function and
// not an interface. This `Sample` function should be provided to the OpenCensus tracing library as
// the `DefaultSampler`, and not the ConstantRateTraceSampler struct.
func (s *ConstantRateTraceSampler) sample(params trace.SamplingParameters) trace.SamplingDecision {
	if params.ParentContext.IsSampled() {
		return trace.SamplingDecision{Sample: true}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Update the number of available samples based on the time since the last check.
	now := s.clock.Now()
	if now.Equal(s.nextRefillTime) || now.After(s.nextRefillTime) {
		durationSinceLastRefillTime := now.Sub(s.lastRefillTime)
		numPeriodsSinceLastRefillTime := max(0, durationSinceLastRefillTime.Nanoseconds()/s.periodDurationNanos)

		s.availableTokens += numPeriodsSinceLastRefillTime * s.tokensPerPeriod
		if s.availableTokens > s.maxTokens {
			s.availableTokens = s.maxTokens
		}

		s.lastRefillTime = s.lastRefillTime.Add(time.Duration(numPeriodsSinceLastRefillTime * s.periodDurationNanos))
		s.nextRefillTime = s.lastRefillTime.Add(time.Duration(s.periodDurationNanos))
	}

	// Sample the trace if there is an available number of tokens.
	if s.availableTokens >= s.tokensPerSample {
		s.availableTokens -= s.tokensPerSample
		return trace.SamplingDecision{Sample: true}
	}

	return trace.SamplingDecision{Sample: false}
}

// NewConstantRateTraceSampler returns a new constant rate trace sampler.
// The period exposed to callers is one second.
func NewConstantRateTraceSampler(tokensPerSecond, maxTokens, tokensPerSample int64, clock clock.Clock) trace.Sampler {
	now := clock.Now()

	s := &ConstantRateTraceSampler{
		clock:               clock,
		availableTokens:     maxTokens,
		maxTokens:           maxTokens,
		periodDurationNanos: int64(time.Second),
		tokensPerPeriod:     tokensPerSecond,
		tokensPerSample:     tokensPerSample,
		lastRefillTime:      now,
		nextRefillTime:      now.Add(time.Second),
	}

	return s.sample
}
