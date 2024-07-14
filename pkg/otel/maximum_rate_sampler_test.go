package otel_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/sdk/trace"

	"go.uber.org/mock/gomock"
)

func TestMaximumRateSampler(t *testing.T) {
	ctrl := gomock.NewController(t)

	clock := mock.NewMockClock(ctrl)
	sampler := otel.NewMaximumRateSampler(clock, 2, time.Second)

	// Initial epoch should start at t = 1000. It should be
	// permitted to start two traces with it. Further sample
	// requests should be dropped.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	require.Equal(t, trace.RecordAndSample, sampler.ShouldSample(trace.SamplingParameters{}).Decision)
	require.Equal(t, trace.RecordAndSample, sampler.ShouldSample(trace.SamplingParameters{}).Decision)

	clock.EXPECT().Now().Return(time.Unix(1000, 500000000))
	require.Equal(t, trace.Drop, sampler.ShouldSample(trace.SamplingParameters{}).Decision)
	clock.EXPECT().Now().Return(time.Unix(1000, 900000000))
	require.Equal(t, trace.Drop, sampler.ShouldSample(trace.SamplingParameters{}).Decision)

	// Second epoch should start at t = 1001.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	require.Equal(t, trace.RecordAndSample, sampler.ShouldSample(trace.SamplingParameters{}).Decision)
	require.Equal(t, trace.RecordAndSample, sampler.ShouldSample(trace.SamplingParameters{}).Decision)

	clock.EXPECT().Now().Return(time.Unix(1001, 100000000))
	require.Equal(t, trace.Drop, sampler.ShouldSample(trace.SamplingParameters{}).Decision)

	// Start a final epoch at t = 1007. Even though no traces were
	// permitted between t = 1001 and 1007, we don't compensate for
	// that, as that would introduce non-deterministic load spikes.
	clock.EXPECT().Now().Return(time.Unix(1007, 0))
	require.Equal(t, trace.RecordAndSample, sampler.ShouldSample(trace.SamplingParameters{}).Decision)
	require.Equal(t, trace.RecordAndSample, sampler.ShouldSample(trace.SamplingParameters{}).Decision)

	clock.EXPECT().Now().Return(time.Unix(1007, 300000000))
	require.Equal(t, trace.Drop, sampler.ShouldSample(trace.SamplingParameters{}).Decision)
}
