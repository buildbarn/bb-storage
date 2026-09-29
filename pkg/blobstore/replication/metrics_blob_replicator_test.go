package replication_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

func TestMetricsBlobReplicatorLabelCardinality(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClock := mock.NewMockClock(ctrl)
	mockReplicator := mock.NewMockBlobReplicator(ctrl)

	// Set up fixed times for predictable testing
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(1 * time.Second)
	gomock.InOrder(
		mockClock.EXPECT().Now().Return(startTime),
		mockClock.EXPECT().Now().Return(endTime),
	)

	storageTypeName := "cas"
	metricsReplicator := replication.NewMetricsBlobReplicator(
		mockReplicator,
		mockClock,
		storageTypeName,
	)

	d := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	digests := digest.NewSetBuilder(0).Add(d).Build()

	// Test ReplicateMultiple
	mockReplicator.EXPECT().
		ReplicateMultiple(gomock.Any(), digests).
		Return(status.Error(codes.Internal, "internal error"))

	// This should not panic due to label cardinality mismatch
	err := metricsReplicator.ReplicateMultiple(context.Background(), digests)
	require.Error(t, err)
}
