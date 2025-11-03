package blobstore

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type readCanaryingCacheEntry struct {
	canaryInProgress    bool
	shouldSendToReplica bool
	expirationTime      time.Time
}

type readCanaryingBlobAccess struct {
	BlobAccess
	replica              BlobAccess
	clock                clock.Clock
	maximumCacheSize     int
	maximumCacheDuration time.Duration
	replicaErrorLogger   util.ErrorLogger

	lock                sync.Mutex
	cachedInstanceNames map[string]readCanaryingCacheEntry
	evictionSet         eviction.Set[string]
}

// NewReadCanaryingBlobAccess creates a decorator for two BlobAccess
// instances. One acts as the source of truth, while the other one acts
// as a read-only replica. This backend may be used to guarantee
// availability in case the replica goes offline.
//
// By default, all requests are sent to the source. For read requests
// (Get and FindMissing), this backend periodically sends a single
// canary request to the replica. Upon success, all subsequent read
// requests are sent to the replica as well. Upon failure, all requests
// will continue to go to the source. Only infrastructure errors are
// considered failures.
//
// State is tracked for each instance name separately. This ensures that
// if the replica uses features like AuthorizingBlobAccess or
// DemultiplexingBlobAccess, this backend still behaves in a meaningful
// way.
func NewReadCanaryingBlobAccess(source, replica BlobAccess, clock clock.Clock, evictionSet eviction.Set[string], maximumCacheSize int, maximumCacheDuration time.Duration, replicaErrorLogger util.ErrorLogger) BlobAccess {
	return &readCanaryingBlobAccess{
		BlobAccess:           source,
		replica:              replica,
		clock:                clock,
		maximumCacheSize:     maximumCacheSize,
		maximumCacheDuration: maximumCacheDuration,
		replicaErrorLogger:   replicaErrorLogger,

		cachedInstanceNames: map[string]readCanaryingCacheEntry{},
		evictionSet:         evictionSet,
	}
}

func (ba *readCanaryingBlobAccess) getAndTouchCacheEntry(instanceNameStr string) (readCanaryingCacheEntry, bool) {
	if entry, ok := ba.cachedInstanceNames[instanceNameStr]; ok {
		// Cache contains a matching entry.
		ba.evictionSet.Touch(instanceNameStr)
		return entry, true
	}

	// Cache contains no matching entry. Free up space, so that the
	// caller may insert a new entry.
	for len(ba.cachedInstanceNames) > ba.maximumCacheSize {
		delete(ba.cachedInstanceNames, ba.evictionSet.Peek())
		ba.evictionSet.Remove()
	}
	ba.evictionSet.Insert(instanceNameStr)
	return readCanaryingCacheEntry{}, false
}

func (ba *readCanaryingBlobAccess) shouldSendToReplica(instanceNameStr string) bool {
	now := ba.clock.Now()

	ba.lock.Lock()
	defer ba.lock.Unlock()

	if entry, ok := ba.getAndTouchCacheEntry(instanceNameStr); ok {
		if entry.canaryInProgress {
			return false
		}
		if now.Before(entry.expirationTime) {
			return entry.shouldSendToReplica
		}
	}

	// No valid cache entry available. Send exactly one request to
	// the replica to check whether it's available. All of the other
	// traffic can go to the source while canarying is in progress.
	ba.cachedInstanceNames[instanceNameStr] = readCanaryingCacheEntry{
		canaryInProgress: true,
	}
	return true
}

func (ba *readCanaryingBlobAccess) recordReplicaResponse(instanceNameStr string, err error) bool {
	gotInfrastructureError := util.IsInfrastructureError(err)
	if gotInfrastructureError {
		ba.replicaErrorLogger.Log(err)
	}
	expirationTime := ba.clock.Now().Add(ba.maximumCacheDuration)

	ba.lock.Lock()
	defer ba.lock.Unlock()

	ba.getAndTouchCacheEntry(instanceNameStr)
	ba.cachedInstanceNames[instanceNameStr] = readCanaryingCacheEntry{
		shouldSendToReplica: !gotInfrastructureError,
		expirationTime:      expirationTime,
	}
	return gotInfrastructureError
}

func (ba *readCanaryingBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	instanceNameStr := d.GetInstanceName().String()
	if ba.shouldSendToReplica(instanceNameStr) {
		return buffer.WithErrorHandler(
			ba.replica.Get(ctx, d),
			&readCanaryingReplicaGetErrorHandler{
				blobAccess: ba,
				context:    ctx,
				digest:     d,
			})
	}
	return buffer.WithErrorHandler(
		ba.BlobAccess.Get(ctx, d),
		readCanaryingSourceErrorHandler{})
}

func (ba *readCanaryingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	instanceNameStr := parentDigest.GetInstanceName().String()
	if ba.shouldSendToReplica(instanceNameStr) {
		return buffer.WithErrorHandler(
			ba.replica.GetFromComposite(ctx, parentDigest, childDigest, slicer),
			&readCanaryingReplicaGetFromCompositeErrorHandler{
				blobAccess:   ba,
				context:      ctx,
				parentDigest: parentDigest,
				childDigest:  childDigest,
				slicer:       slicer,
			})
	}
	return buffer.WithErrorHandler(
		ba.BlobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer),
		readCanaryingSourceErrorHandler{})
}

func (ba *readCanaryingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// The backend may behave differently based on the REv2 instance
	// name that is, for example if AuthorizingBlobAccess and
	// DemultiplexingBlobAccess are used. Because the set may use
	// multiple instance names, we must decompose the request to get
	// accurate error responses.
	digestsByInstanceName := digests.PartitionByInstanceName()
	digestsForSource := make([]digest.Set, 0, len(digestsByInstanceName))
	missingFromReplicas := make([]digest.Set, 0, len(digestsByInstanceName))
	for _, digestsForInstanceName := range digestsByInstanceName {
		instanceNameStr := digestsForInstanceName.Items()[0].GetInstanceName().String()
		if ba.shouldSendToReplica(instanceNameStr) {
			missingFromReplica, err := ba.replica.FindMissing(ctx, digestsForInstanceName)
			if !ba.recordReplicaResponse(instanceNameStr, err) {
				if err != nil {
					return digest.EmptySet, util.StatusWrapf(err, "Replica, instance name %#v", instanceNameStr)
				}
				missingFromReplicas = append(missingFromReplicas, missingFromReplica)
				continue
			}
		}
		// Replica was known to be unavailable, or a call
		// against it just failed. Forward the request to the
		// source.
		digestsForSource = append(digestsForSource, digestsForInstanceName)
	}

	// Single request to the source for all instance names for which
	// the replica is unavailable.
	missingFromSource, err := ba.BlobAccess.FindMissing(ctx, digest.GetUnion(digestsForSource))
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Source")
	}
	return digest.GetUnion(append(missingFromReplicas, missingFromSource)), nil
}

// readCanaryingReplicaGetErrorHandler is the ErrorHandler that is
// attached to all buffers read from the replica backend through the
// Get() operation.
type readCanaryingReplicaGetErrorHandler struct {
	blobAccess *readCanaryingBlobAccess
	context    context.Context
	digest     digest.Digest
}

func (eh *readCanaryingReplicaGetErrorHandler) OnError(err error) (buffer.Buffer, error) {
	ba := eh.blobAccess
	if ba == nil {
		// Already retried the operation against the source backend.
		return nil, util.StatusWrap(err, "Source")
	}
	eh.blobAccess = nil
	if ba.recordReplicaResponse(eh.digest.GetInstanceName().String(), err) {
		// Request against the replica failed with an
		// infrastructure error. Retry it against the source
		// backend.
		return ba.BlobAccess.Get(eh.context, eh.digest), nil
	}
	return nil, util.StatusWrap(err, "Replica")
}

func (eh *readCanaryingReplicaGetErrorHandler) Done() {
	if ba := eh.blobAccess; ba != nil {
		ba.recordReplicaResponse(eh.digest.GetInstanceName().String(), nil)
	}
}

// readCanaryingReplicaGetFromCompositeErrorHandler is the ErrorHandler
// that is attached to all buffers read from the replica backend through
// the GetFromComposite() operation.
type readCanaryingReplicaGetFromCompositeErrorHandler struct {
	blobAccess   *readCanaryingBlobAccess
	context      context.Context
	parentDigest digest.Digest
	childDigest  digest.Digest
	slicer       slicing.BlobSlicer
}

func (eh *readCanaryingReplicaGetFromCompositeErrorHandler) OnError(err error) (buffer.Buffer, error) {
	ba := eh.blobAccess
	if ba == nil {
		// Already retried the operation against the source backend.
		return nil, util.StatusWrap(err, "Source")
	}
	eh.blobAccess = nil
	if ba.recordReplicaResponse(eh.parentDigest.GetInstanceName().String(), err) {
		// Request against the replica failed with an
		// infrastructure error. Retry it against the source
		// backend.
		return ba.BlobAccess.GetFromComposite(eh.context, eh.parentDigest, eh.childDigest, eh.slicer), nil
	}
	return nil, util.StatusWrap(err, "Replica")
}

func (eh *readCanaryingReplicaGetFromCompositeErrorHandler) Done() {
	if ba := eh.blobAccess; ba != nil {
		ba.recordReplicaResponse(eh.parentDigest.GetInstanceName().String(), nil)
	}
}

// readCanaryingSourceErrorHandler is the ErrorHandler that is attached
// to all buffers read from the source backend.
type readCanaryingSourceErrorHandler struct{}

func (readCanaryingSourceErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, util.StatusWrap(err, "Source")
}

func (readCanaryingSourceErrorHandler) Done() {}
