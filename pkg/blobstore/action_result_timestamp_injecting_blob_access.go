package blobstore

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	actionResultTimestampInjectingBlobAccessOperationsPrometheusMetrics sync.Once

	actionResultTimestampInjectingBlobAccessPutOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "action_result_timestamp_injecting_blob_access_put_operations_total",
			Help:      "Number of Put() operations performed against the Action Cache, and how many of those had execution_metadata.worker_completed_timestamp set.",
		},
		[]string{"worker_completed_timestamp"})
	actionResultTimestampInjectingBlobAccessPutOperationsPresent = actionResultTimestampInjectingBlobAccessPutOperations.WithLabelValues("present")
	actionResultTimestampInjectingBlobAccessPutOperationsAbsent  = actionResultTimestampInjectingBlobAccessPutOperations.WithLabelValues("absent")
)

type actionResultTimestampInjectingBlobAccess struct {
	BlobAccess
	clock                   clock.Clock
	maximumMessageSizeBytes int
}

// NewActionResultTimestampInjectingBlobAccess creates a decorator for
// an Action Cache (AC) that for each ActionResult message written
// through it, sets the execution_metadata.worker_completed_timestamp
// field to the current time, if not set already.
//
// This decorator is necessary to make ActionResultExpiringBlobAccess
// work reliably, as it depends on this field being set. Not all clients
// set this field.
func NewActionResultTimestampInjectingBlobAccess(blobAccess BlobAccess, clock clock.Clock, maximumMessageSizeBytes int) BlobAccess {
	actionResultTimestampInjectingBlobAccessOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(actionResultTimestampInjectingBlobAccessPutOperations)
	})

	return &actionResultTimestampInjectingBlobAccess{
		BlobAccess:              blobAccess,
		clock:                   clock,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (ba *actionResultTimestampInjectingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	b1, b2 := b.CloneCopy(ba.maximumMessageSizeBytes)
	actionResultMessage, err := b1.ToProto(&remoteexecution.ActionResult{}, ba.maximumMessageSizeBytes)
	if err != nil {
		b2.Discard()
		return err
	}

	oldActionResult := actionResultMessage.(*remoteexecution.ActionResult)
	if oldActionResult.ExecutionMetadata.GetWorkerCompletedTimestamp() != nil {
		// Timestamp is present. Forward the ActionResult unaltered.
		actionResultTimestampInjectingBlobAccessPutOperationsPresent.Inc()
		return ba.BlobAccess.Put(ctx, digest, b2)
	}

	// Timestamp is absent. Inject the current time into it.
	actionResultTimestampInjectingBlobAccessPutOperationsAbsent.Inc()
	b2.Discard()
	newActionResult := &remoteexecution.ActionResult{
		ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
			WorkerCompletedTimestamp: timestamppb.New(ba.clock.Now()),
		},
	}
	proto.Merge(newActionResult, oldActionResult)
	return ba.BlobAccess.Put(ctx, digest, buffer.NewProtoBufferFromProto(newActionResult, buffer.UserProvided))
}
