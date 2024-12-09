package replication

import (
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"go.uber.org/mock/gomock"
)

func TestNewMetricsBlobReplicator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock using the generated mock
	mockReplicator := mock.NewMockBlobReplicator(ctrl)
	mockClock := mock.NewMockClock(ctrl)

	// Create a new MetricsBlobReplicator
	storageTypeName := "cas"
	metricsReplicator := replication.NewMetricsBlobReplicator(mockReplicator, mockClock, storageTypeName)
	require.NotNil(t, metricsReplicator)
}
