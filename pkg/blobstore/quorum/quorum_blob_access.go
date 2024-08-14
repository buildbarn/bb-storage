package quorum

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	quorumBlobAccessPrometheusMetrics sync.Once

	quorumBlobAccessFindMissingSynchronizations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "quorum_blob_access_find_missing_synchronizations",
			Help:      "Number of blobs synchronized in FindMissing()",
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1.0, 2.0, 16)...),
		},
		[]string{"direction"})
)

type quorumBlobAccess struct {
	backends    []blobstore.BlobAccess
	readQuorum  int
	writeQuorum int
	generator   random.ThreadSafeGenerator
}

// NewQuorumBlobAccess creates a BlobAccess that applies operations to a subset
// of storage backends, retrying on infrastructure errors.  Read and write quorum
// size should be chosen so that they overlap by at least one backend.
// Note: Data is not replicated again after the original write.
func NewQuorumBlobAccess(backends []blobstore.BlobAccess, readQuorum, writeQuorum int) blobstore.BlobAccess {
	quorumBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(quorumBlobAccessFindMissingSynchronizations)
	})

	return &quorumBlobAccess{
		backends:    backends,
		readQuorum:  readQuorum,
		writeQuorum: writeQuorum,
		generator:   random.FastThreadSafeGenerator,
	}
}

func (ba *quorumBlobAccess) shuffledBackends() []blobstore.BlobAccess {
	backends := make([]blobstore.BlobAccess, len(ba.backends))
	copy(backends, ba.backends)

	ba.generator.Shuffle(len(backends), func(i, j int) {
		backends[i], backends[j] = backends[j], backends[i]
	})

	return backends
}

type getQuorumErrorHandler struct {
	remainingBackends []blobstore.BlobAccess
	remainingReads    int
	retry             func(blobstore.BlobAccess) buffer.Buffer
}

func (eh *getQuorumErrorHandler) tryNextBackendOrError(err error) (buffer.Buffer, error) {
    if len(eh.remainingBackends) > 0 {
        nextBackend := eh.remainingBackends[0]
        eh.remainingBackends = eh.remainingBackends[1:]
        return eh.retry(nextBackend), nil
    }
    return nil, err
}

func (eh *getQuorumErrorHandler) OnError(err error) (buffer.Buffer, error) {
    fallbackErr := status.Error(codes.Unavailable, "Too many backends unavailable")
    if util.IsInfrastructureError(err) {
        // I/O error.  Try again on another backend.
        return eh.tryNextBackendOrError(fallbackErr)

    } else if status.Code(err) == codes.NotFound {
        // Not found.  Try again on another backend - if we haven't seen enough yet.
        if eh.remainingReads <= 1 {
            // Observed sufficient NotFounds.  Return conclusive NotFound.
            return nil, err
        }
        eh.remainingReads--

        // Haven't been able to check enough backends.  Can't conclude not found.
        return eh.tryNextBackendOrError(fallbackErr)
    }

	return nil, err
}

func (eh getQuorumErrorHandler) Done() {}

func (ba *quorumBlobAccess) get(getter func(b blobstore.BlobAccess) buffer.Buffer) buffer.Buffer {
	backends := ba.shuffledBackends()

	backend := backends[0]
	remainingBackends := backends[1:]

	return buffer.WithErrorHandler(
		getter(backend),
		&getQuorumErrorHandler{
			remainingBackends: remainingBackends,
			remainingReads:    ba.readQuorum,
			retry:             getter,
		})
}

func (ba *quorumBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return ba.get(func(b blobstore.BlobAccess) buffer.Buffer {
		return b.Get(ctx, digest)
	})
}

func (ba *quorumBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return ba.get(func(b blobstore.BlobAccess) buffer.Buffer {
		return b.GetFromComposite(ctx, parentDigest, childDigest, slicer)
	})
}

func (ba *quorumBlobAccess) shuffledBackendQueue() <-chan blobstore.BlobAccess {
	queue := make(chan blobstore.BlobAccess)
	go func() error {
		backends := ba.shuffledBackends()

		for _, b := range backends {
			queue <- b
		}

		close(queue)
		return nil
	}()
    return queue
}

func (ba *quorumBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	// Store object in at least writeQuorum storage backends.
	group, ctx := errgroup.WithContext(ctx)
    backendQueue := ba.shuffledBackendQueue()

	// Spawn writeQuorum writers.  Each of these goroutines needs to succeed once.
	for i := 0; i < ba.writeQuorum; i++ {
		var b1 buffer.Buffer
		if i == ba.writeQuorum-1 {
			// Last writer, no need to clone buffer
			b1 = b
		} else {
			b, b1 = b.CloneStream()
		}

		group.Go(func() error {
			var err error
			for backend := range backendQueue {
				err = backend.Put(ctx, digest, b1)
				if err == nil {
					// Success
					return nil
				}
			}
			return err
		})
	}

	return group.Wait()
}

func (ba *quorumBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Call FindMissing() on readQuorum backends.
	group, ctx := errgroup.WithContext(ctx)
    backendQueue := ba.shuffledBackendQueue()

	results := make([]digest.Set, ba.readQuorum)
	for i := 0; i < ba.readQuorum; i++ {
		resultIdx := i
		group.Go(func() error {
			var err error
			for backend := range backendQueue {
				results[resultIdx], err = backend.FindMissing(ctx, digests)
				if err == nil {
					// Success
					return nil
				}
			}
			return err
		})
	}

	if err := group.Wait(); err != nil {
		return digest.EmptySet, err
	}

	// Find intersection of all results
	missingFromAll := results[0]
	for _, result := range results[1:] {
		_, missingFromAll, _ = digest.GetDifferenceAndIntersection(missingFromAll, result)
	}
	return missingFromAll, nil
}

func (ba *quorumBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	backends := ba.shuffledBackends()

	return backends[0].GetCapabilities(ctx, instanceName)
}
