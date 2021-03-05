package global_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/global"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"

	"go.opencensus.io/trace"
)

func TestConstantRateTraceSampler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	clock := mock.NewMockClock(ctrl)

	ts := time.Unix(1000, 0)

	params := trace.SamplingParameters{}

	clock.EXPECT().Now().Return(ts)
	s := global.NewConstantRateTraceSampler(2, 2, 1, clock)

	// The bucket has 2 tokens. Thus, we should be able to consume both tokens and no more.
	clock.EXPECT().Now().Return(ts).Times(3)
	assert.True(t, s(params).Sample)
	assert.True(t, s(params).Sample)
	assert.False(t, s(params).Sample)

	// Advance time 0.25 seconds. There should still be not enough tokens.
	clock.EXPECT().Now().Return(ts.Add(time.Second / 4))
	assert.False(t, s(params).Sample)

	// Advance to the next second. There should be enough tokens now.
	clock.EXPECT().Now().Return(ts.Add(time.Second)).Times(3)
	assert.True(t, s(params).Sample)
	assert.True(t, s(params).Sample)
	assert.False(t, s(params).Sample)

	// Advance time 5 seconds. The "max tokens" cap will apply.
	clock.EXPECT().Now().Return(ts.Add(5 * time.Second)).Times(5)
	assert.True(t, s(params).Sample)
	assert.True(t, s(params).Sample)
	assert.False(t, s(params).Sample)
	assert.False(t, s(params).Sample)
	assert.False(t, s(params).Sample)
}
