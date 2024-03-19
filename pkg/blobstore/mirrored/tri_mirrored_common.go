package mirrored

// TODO(ragost): are connections re-established automatically after a failure?

import (
	"context"
	"fmt"
	"log"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	A_IDX    = 0 // backend A
	B_IDX    = 1 // backend B
	C_IDX    = 2 // backend C
	N_MIRROR = 3
	MAX_ERR  = 1 // max number of backend errors we can withstand per operation
)

var (
	triMirroredBlobAccessPrometheusMetrics sync.Once

	triMirroredMissingBlobReplicationCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_missing_blob_replication_total",
			Help:      "Number of times missing blobs were replicated to other backends",
		})
	triMirroredMissingBlobReplicationErrorCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_missing_blob_replication_error_total",
			Help:      "Number of times replicating missing blobs to other backends failed",
		})
	triMirroredBlobInvalidationCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_blob_invalidation_total",
			Help:      "Number of times blobs were invalidated",
		})
	triMirroredBlobInvalidationErrorCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_blob_invalidation_error_total",
			Help:      "Number of times blobs invalidation attempts failed",
		})
	triMirroredBlobRaceCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_blob_race_total",
			Help:      "Number of times races were detected",
		})
	triMirroredBlobRaceRetryFailedCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_blob_race_retry_failed_total",
			Help:      "Number of times races retries failed to result in a quorum",
		})
	triMirroredBlobReadHandleErrorRetryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_blob_read_handle_error_retry_total",
			Help:      "Number of times read error handlers retried the operation",
		})
	triMirroredBlobReadHandleErrorFailedCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "trimirror_blob_read_handle_error_failed_total",
			Help:      "Number of times read error handlers failed to retry the operation",
		})
)

func backendName(idx int) string {
	switch idx {
	case 0:
		return "Backend A"
	case 1:
		return "Backend B"
	case 2:
		return "Backend C"
	default:
		return "Unknown backend"
	}
}

// NewTriMirroredBlobAccess creates a BlobAccess that applies operations to
// three storage backends in such a way that they are mirrored. When
// inconsistencies between the three storage backends are detected (i.e.,
// a blob is only present in one of the backends), the blob is replicated.
func NewTriMirroredBlobAccess(backendA, backendB, backendC blobstore.BlobAccess, storageType pb.StorageType_Value) blobstore.BlobAccess {
	triMirroredBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(triMirroredMissingBlobReplicationCount)
		prometheus.MustRegister(triMirroredMissingBlobReplicationErrorCount)
		prometheus.MustRegister(triMirroredBlobInvalidationCount)
		prometheus.MustRegister(triMirroredBlobInvalidationErrorCount)
		prometheus.MustRegister(triMirroredBlobRaceCount)
		prometheus.MustRegister(triMirroredBlobRaceRetryFailedCount)
		prometheus.MustRegister(triMirroredBlobReadHandleErrorRetryCount)
		prometheus.MustRegister(triMirroredBlobReadHandleErrorFailedCount)
	})

	if storageType == pb.StorageType_ACTION_CACHE {
		return &triMirroredACBlobAccess{
			backends: [N_MIRROR]blobstore.BlobAccess{backendA, backendB, backendC},
		}
	} else if storageType == pb.StorageType_CASTORE {
		return &triMirroredCASBlobAccess{
			backends: [N_MIRROR]blobstore.BlobAccess{backendA, backendB, backendC},
		}
	} else {
		panic("Unknown TriMirror storage type")
	}
}

type concurrentBlobRead struct {
	bufs     [N_MIRROR]*buffer.Buffer
	errors   [N_MIRROR]error
	digest   digest.Digest
	bufCount int
	errCount int
	srcIdx   int
	done     *sync.Cond
}

// TODO(ragost): for now, NOT_FOUND is the only error for which we need to replicate blobs.  There might
// be others, for example, retriable (transient) errors.  Figure out which those are.
func shouldCopy(err error) bool {
	if status.Code(err) == codes.NotFound {
		return true
	}
	return false
}

func invalidateBuf(b buffer.Buffer) (*buffer.Buffer, error) {
	sz, err := b.GetSizeBytes()
	if err != nil {
		log.Printf("Can't get buffer size for invalidate: %v", err)
		return nil, err
	}
	pr, err := b.ToProto(&remoteexecution.ActionResult{}, int(sz))
	if err != nil {
		log.Printf("Failed converting buffer to proto: %v", err)
		return nil, err
	}
	actionResult, ok := pr.(*remoteexecution.ActionResult)
	if !ok {
		log.Printf("Can't convert protoreflect.Message into concrete message type")
		return nil, fmt.Errorf("Can't convert protoreflect.Message into concrete message type")
	}
	actionResult.ExitCode = 667 // any nonzero exit code will do
	newBuf := buffer.NewProtoBufferFromProto(actionResult, buffer.UserProvided)
	return &newBuf, nil
}

func invalidate(ctx context.Context, digest digest.Digest, b buffer.Buffer, ba blobstore.BlobAccess) error {
	nb, err := invalidateBuf(b)
	if err != nil {
		return err
	}
	err = ba.Put(ctx, digest, *nb)
	if err != nil {
		log.Printf("Error writing invalidated AC entry to backend: %v", err)
		triMirroredBlobInvalidationErrorCount.Inc()
	} else {
		triMirroredBlobInvalidationCount.Inc()
	}
	return err
}
