package blobstore

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
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
	source               BlobAccess
	replica              BlobAccess
	clock                clock.Clock
	maximumCacheSize     int
	maximumCacheDuration time.Duration

	lock                sync.Mutex
	cachedInstanceNames map[string]readCanaryingCacheEntry
	evictionSet         eviction.Set
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
func NewReadCanaryingBlobAccess(source, replica BlobAccess, clock clock.Clock, evictionSet eviction.Set, maximumCacheSize int, maximumCacheDuration time.Duration) BlobAccess {
	return &readCanaryingBlobAccess{
		source:               source,
		replica:              replica,
		clock:                clock,
		maximumCacheSize:     maximumCacheSize,
		maximumCacheDuration: maximumCacheDuration,

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

func (ba *readCanaryingBlobAccess) recordReplicaResponse(instanceNameStr string, err error) {
	shouldSendToReplica := !util.IsInfrastructureError(err)
	expirationTime := ba.clock.Now().Add(ba.maximumCacheDuration)

	ba.lock.Lock()
	defer ba.lock.Unlock()

	ba.getAndTouchCacheEntry(instanceNameStr)
	ba.cachedInstanceNames[instanceNameStr] = readCanaryingCacheEntry{
		shouldSendToReplica: shouldSendToReplica,
		expirationTime:      expirationTime,
	}
}

func (ba *readCanaryingBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	instanceNameStr := d.GetInstanceName().String()
	if ba.shouldSendToReplica(instanceNameStr) {
		return buffer.WithErrorHandler(
			ba.replica.Get(ctx, d),
			&readCanaryingReplicaErrorHandler{
				blobAccess:      ba,
				instanceNameStr: instanceNameStr,
			})
	}
	return buffer.WithErrorHandler(
		ba.source.Get(ctx, d),
		readCanaryingSourceErrorHandler{})
}

func (ba *readCanaryingBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	// There is no point in writing objects to the replica, as the
	// replica always has to forward it to the source. We therefore
	// write to the source directly.
	return ba.source.Put(ctx, d, b)
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
			ba.recordReplicaResponse(instanceNameStr, err)
			if err != nil {
				return digest.EmptySet, util.StatusWrapf(err, "Replica, instance name %#v", instanceNameStr)
			}
			missingFromReplicas = append(missingFromReplicas, missingFromReplica)
		} else {
			digestsForSource = append(digestsForSource, digestsForInstanceName)
		}
	}

	// Single request to the source for all instance names for which
	// the replica is unavailable.
	missingFromSource, err := ba.source.FindMissing(ctx, digest.GetUnion(digestsForSource))
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Source")
	}
	return digest.GetUnion(append(missingFromReplicas, missingFromSource)), nil
}

// readCanaryingReplicaErrorHandler is the ErrorHandler that is attached
// to all buffers read from the replica backend.
type readCanaryingReplicaErrorHandler struct {
	blobAccess      *readCanaryingBlobAccess
	instanceNameStr string
}

func (eh *readCanaryingReplicaErrorHandler) OnError(err error) (buffer.Buffer, error) {
	eh.blobAccess.recordReplicaResponse(eh.instanceNameStr, err)
	eh.blobAccess = nil
	return nil, util.StatusWrap(err, "Replica")
}

func (eh *readCanaryingReplicaErrorHandler) Done() {
	if ba := eh.blobAccess; ba != nil {
		ba.recordReplicaResponse(eh.instanceNameStr, nil)
	}
}

// readCanaryingSourceErrorHandler is the ErrorHandler that is attached
// to all buffers read from the source backend.
type readCanaryingSourceErrorHandler struct{}

func (eh readCanaryingSourceErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, util.StatusWrap(err, "Source")
}

func (eh readCanaryingSourceErrorHandler) Done() {}
