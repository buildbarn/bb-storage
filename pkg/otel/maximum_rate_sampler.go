package otel

import (
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"

	sdk_trace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type maximumRateSampler struct {
	clock           clock.Clock
	samplesPerEpoch int
	epochDuration   time.Duration

	lock             sync.Mutex
	samplesRemaining int
	epochEnd         time.Time
}

// NewMaximumRateSampler creates an OpenTelemetry Sampler that only
// permits sampling traces according to a maximum rate. This ensures
// that the amount of traffic sent to the tracing infrastructure remains
// bounded.
func NewMaximumRateSampler(clock clock.Clock, samplesPerEpoch int, epochDuration time.Duration) sdk_trace.Sampler {
	return &maximumRateSampler{
		clock:           clock,
		samplesPerEpoch: samplesPerEpoch,
		epochDuration:   epochDuration,
	}
}

func (s *maximumRateSampler) getSamplingDecision() sdk_trace.SamplingDecision {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.samplesRemaining > 0 {
		// Current epoch still permits one or more samples.
		s.samplesRemaining--
		return sdk_trace.RecordAndSample
	}
	if now := s.clock.Now(); !now.Before(s.epochEnd) {
		// Enter the next epoch.
		s.samplesRemaining = s.samplesPerEpoch - 1
		s.epochEnd = now.Add(s.epochDuration)
		return sdk_trace.RecordAndSample
	}
	return sdk_trace.Drop
}

func (s *maximumRateSampler) ShouldSample(p sdk_trace.SamplingParameters) sdk_trace.SamplingResult {
	return sdk_trace.SamplingResult{
		Decision:   s.getSamplingDecision(),
		Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (s *maximumRateSampler) Description() string {
	return "MaximumRateSampler"
}
