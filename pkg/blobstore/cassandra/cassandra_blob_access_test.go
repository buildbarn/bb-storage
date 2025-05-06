package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_estimateSegmentCount(t *testing.T) {
	tests := []struct {
		segmentSize int32
		sizeInBytes int64
		want        int
	}{
		{64, 1, 1},
		{64, (12 * 64) - 1, 12},
		{64, (12 * 64), 12},
		{64, (12 * 64) + 1, 13},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.want, estimateSegmentCount(tt.segmentSize, tt.sizeInBytes))
		})
	}
}
