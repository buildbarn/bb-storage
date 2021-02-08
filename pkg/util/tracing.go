package util

import (
	"sync"
	"time"

	"go.opencensus.io/trace"
)

// ConstantRateTraceSampler uses a token bucket algorithm to sample traces. This is useful as an alternative
// to probability-based sampling when the service has a low number of requests per second.
type ConstantRateTraceSampler struct {
	lock sync.Mutex

	// The number of available tokens in the bucket.
	AvailableTokens int64

	// Maximum number of tokens in the bucket.
	MaxTokens int64

	// Size of a period in nanoseconds.
	PeriodDurationNanos int64

	// Number of tokens put into the bucket per time period.
	TokensPerPeriod int64

	// Number of tokens required to sample a trace.
	TokensPerSample int64

	LastRefillTime time.Time
	NextRefillTime time.Time
	ClockNow       func() time.Time
}

func max(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

// Sample decides whether to sample a trace. The trace.Sampler type is unfortunately a function and
// not an interface. This `Sample` function should be provided to the OpenCensus tracing library as
// the `DefaultSampler`, and not the ConstantRateTraceSampler struct.
func (s *ConstantRateTraceSampler) Sample(params trace.SamplingParameters) trace.SamplingDecision {
	if params.ParentContext.IsSampled() {
		return trace.SamplingDecision{Sample: true}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Update the number of available samples based on the time since the last check.
	now := s.ClockNow()
	if now.Equal(s.NextRefillTime) || now.After(s.NextRefillTime) {
		durationSinceLastRefillTime := now.Sub(s.LastRefillTime)
		numPeriodsSinceLastRefillTime := max(0, durationSinceLastRefillTime.Nanoseconds()/s.PeriodDurationNanos)

		s.AvailableTokens += numPeriodsSinceLastRefillTime * s.TokensPerPeriod
		if s.AvailableTokens > s.MaxTokens {
			s.AvailableTokens = s.MaxTokens
		}

		s.LastRefillTime = s.LastRefillTime.Add(time.Duration(numPeriodsSinceLastRefillTime * s.PeriodDurationNanos))
		s.NextRefillTime = s.LastRefillTime.Add(time.Duration(s.PeriodDurationNanos))
	}

	// Sample the trace if there is an available number of tokens.
	if s.AvailableTokens >= s.TokensPerSample {
		s.AvailableTokens -= s.TokensPerSample
		return trace.SamplingDecision{Sample: true}
	}

	return trace.SamplingDecision{Sample: false}
}

// NewConstantRateTraceSampler returns a new constant rate trace sampler.
// The period exposed to callers is to one second.
func NewConstantRateTraceSampler(tokensPerSecond, maxTokens, tokensPerSample int64) *ConstantRateTraceSampler {
	now := time.Now()

	s := &ConstantRateTraceSampler{
		AvailableTokens:     maxTokens,
		MaxTokens:           maxTokens,
		PeriodDurationNanos: int64(time.Second),
		TokensPerPeriod:     tokensPerSecond,
		TokensPerSample:     tokensPerSample,
		LastRefillTime:      now,
		NextRefillTime:      now.Add(time.Second),
		ClockNow:            time.Now,
	}

	return s
}
