package util_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/assert"
	"go.opencensus.io/trace"
)

func TestConstantRateTraceSampler(t *testing.T) {
	params := trace.SamplingParameters{}

	ts := time.Now()

	s := util.NewConstantRateTraceSampler(2, 2, 1)
	s.LastRefillTime = ts
	s.NextRefillTime = s.LastRefillTime.Add(time.Duration(s.PeriodDurationNanos))
	s.ClockNow = func() time.Time {
		return ts
	}

	// The bucket has 2 tokens. Thus, we should be able to consume both tokens and no more.
	assert.True(t, s.Sample(params).Sample)
	assert.True(t, s.Sample(params).Sample)
	assert.False(t, s.Sample(params).Sample)

	// Advance time 0.25 seconds. There should not be enough tokens.
	s.ClockNow = func() time.Time {
		return ts.Add(time.Second / 4)
	}
	assert.False(t, s.Sample(params).Sample)

	// Advance to the next second. There should be enough tokens now.
	s.ClockNow = func() time.Time {
		return ts.Add(time.Second)
	}
	assert.True(t, s.Sample(params).Sample)
	assert.True(t, s.Sample(params).Sample)
	assert.False(t, s.Sample(params).Sample)

	// Advance time 5 seconds. The "max tokens" cap will apply.
	s.ClockNow = func() time.Time {
		return ts.Add(5 * time.Second)
	}
	assert.True(t, s.Sample(params).Sample)
	assert.True(t, s.Sample(params).Sample)
	assert.False(t, s.Sample(params).Sample)
	assert.False(t, s.Sample(params).Sample)
	assert.False(t, s.Sample(params).Sample)
}
