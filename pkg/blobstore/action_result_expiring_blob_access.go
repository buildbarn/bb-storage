package blobstore

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionResultExpiringBlobAccess struct {
	BlobAccess
	clock                   clock.Clock
	maximumMessageSizeBytes int
	minimumValidity         time.Duration
	maximumValidityJitter   uint64
}

// NewActionResultExpiringBlobAccess creates a decorator for an Action
// Cache (AC) that automatically expires ActionResult objects after a
// certain amount of time has passed. This forces targets to be rebuilt
// periodically.
//
// The expiration time of an ActionResult is computed by considering the
// 'worker_completed_timestamp' field in ExecutedActionMetadata. Jitter
// is added to the expiration time to amortize rebuilds. The process for
// determining the amount of jitter is deterministic, meaning that it is
// safe to use this decorator in a distributed setting.
func NewActionResultExpiringBlobAccess(blobAccess BlobAccess, clock clock.Clock, maximumMessageSizeBytes int, minimumValidity, maximumValidityJitter time.Duration) BlobAccess {
	return &actionResultExpiringBlobAccess{
		BlobAccess:              blobAccess,
		clock:                   clock,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		minimumValidity:         minimumValidity,
		maximumValidityJitter:   uint64(maximumValidityJitter),
	}
}

func (ba *actionResultExpiringBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	b1, b2 := ba.BlobAccess.Get(ctx, digest).CloneCopy(ba.maximumMessageSizeBytes)
	actionResultMessage, err := b1.ToProto(&remoteexecution.ActionResult{}, ba.maximumMessageSizeBytes)
	if err != nil {
		b2.Discard()
		return buffer.NewBufferFromError(err)
	}
	actionResult := actionResultMessage.(*remoteexecution.ActionResult)
	if workerCompletedTimestamp := actionResult.ExecutionMetadata.GetWorkerCompletedTimestamp(); workerCompletedTimestamp.CheckValid() == nil {
		// ActionResult has a valid 'worker_completed_timestamp'
		// field. Pick an expiration time that includes jitter.
		t := workerCompletedTimestamp.AsTime()
		expirationTime := t.Add(ba.minimumValidity).Add(time.Duration(uint64(t.Unix()) * 0x936a0d2a41e8c779 % ba.maximumValidityJitter))
		if ba.clock.Now().After(expirationTime) {
			b2.Discard()
			return buffer.NewBufferFromError(status.Errorf(codes.NotFound, "Action result expired at %s", expirationTime.Format(time.RFC3339)))
		}
	}
	return b2
}
