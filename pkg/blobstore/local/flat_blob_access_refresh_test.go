package local_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/lossymap"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The tests in this file check that a refresh never replaces a value
// written by a Put() that overlaps with it. They use the real
// OldCurrentNewLocationBlobMap, VolatileBlockList and hash map,
// configured the way an Action Cache is (one "new" block, mutable
// growth policy). Interleavings are made exact by holding a chosen
// write or Decode() call open.

const refreshTestBlockSize = 4096

// gatedLocationBlobMap holds the copy of the next Put() open until
// released, after signaling that its space has been allocated.
type gatedLocationBlobMap struct {
	local.LocationBlobMap

	lock      sync.Mutex
	gate      chan struct{}
	allocated chan struct{}
}

func (m *gatedLocationBlobMap) arm() (gate, allocated chan struct{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.gate, m.allocated = make(chan struct{}), make(chan struct{})
	return m.gate, m.allocated
}

func (m *gatedLocationBlobMap) disarm() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.gate, m.allocated = nil, nil
}

func (m *gatedLocationBlobMap) Put(sizeBytes int64) (local.LocationBlobPutWriter, error) {
	putWriter, err := m.LocationBlobMap.Put(sizeBytes)
	m.lock.Lock()
	gate, allocated := m.gate, m.allocated
	m.gate, m.allocated = nil, nil
	m.lock.Unlock()
	if err != nil || gate == nil {
		return putWriter, err
	}
	close(allocated)
	return func(data []byte) local.LocationBlobPutFinalizer {
		<-gate
		return putWriter(data)
	}, nil
}

// gatedCoder holds the next Decode() open until released. A refresh
// decodes the blob after reading it and before re-checking its
// location, so this pauses it between the two.
type gatedCoder struct {
	coder.Coder[*remoteexecution.ActionResult, []byte]

	lock    sync.Mutex
	gate    chan struct{}
	reached chan struct{}
}

func (c *gatedCoder) arm() (gate, reached chan struct{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.gate, c.reached = make(chan struct{}), make(chan struct{})
	return c.gate, c.reached
}

func (c *gatedCoder) Decode(data []byte, blobDigest digest.Digest) (*remoteexecution.ActionResult, error) {
	c.lock.Lock()
	gate, reached := c.gate, c.reached
	c.gate, c.reached = nil, nil
	c.lock.Unlock()
	if gate != nil {
		close(reached)
		<-gate
	}
	return c.Coder.Decode(data, blobDigest)
}

type refreshTestStore struct {
	blobAccess      blobstore.BlobAccess[*remoteexecution.ActionResult]
	keyLocationMap  local.KeyLocationMap
	locationBlobMap *local.OldCurrentNewLocationBlobMap
	gatedPuts       *gatedLocationBlobMap
	gatedDecodes    *gatedCoder
	lock            *sync.RWMutex
	digestKeyFormat digest.KeyFormat
}

// refreshTestRecordKeyHasher is the hasher used by
// pkg/blobstore/configuration, with a fixed initialization value.
func refreshTestRecordKeyHasher(k *lossymap.RecordKey[local.Key]) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range k.Key {
		h ^= uint64(c)
		h *= 1099511628211
	}
	attempt := k.Attempt
	for i := 0; i < 4; i++ {
		h ^= uint64(attempt & 0xff)
		h *= 1099511628211
		attempt >>= 8
	}
	return h
}

// refreshTestLocationComparator is the comparator used by
// pkg/blobstore/configuration.
func refreshTestLocationComparator(a, b *local.Location) int {
	if a.BlockIndex < b.BlockIndex {
		return -1
	}
	if a.BlockIndex > b.BlockIndex {
		return 1
	}
	if a.OffsetBytes < b.OffsetBytes {
		return -1
	}
	if a.OffsetBytes > b.OffsetBytes {
		return 1
	}
	return 0
}

func newRefreshTestStore(t *testing.T, recordCount uint64, maximumGetAttempts uint8, maximumPutAttempts int, digestKeyFormat digest.KeyFormat) *refreshTestStore {
	locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
		local.NewVolatileBlockList(local.NewInMemoryBlockAllocator(refreshTestBlockSize)),
		local.NewMutableBlockListGrowthPolicy(1),
		util.DefaultErrorLogger,
		"AC",
		refreshTestBlockSize,
		/* oldBlocksCount = */ 1,
		/* newBlocksCount = */ 1,
		/* initialBlocksCount = */ 0,
	)
	keyLocationMap := lossymap.NewHashMap(
		local.NewInMemoryLocationRecordArray(int(recordCount)),
		refreshTestRecordKeyHasher,
		recordCount,
		refreshTestLocationComparator,
		maximumGetAttempts,
		maximumPutAttempts,
		"KeyLocationMap:FlatBlobAccessRefreshTest",
	)
	s := &refreshTestStore{
		keyLocationMap:  keyLocationMap,
		locationBlobMap: locationBlobMap,
		gatedPuts:       &gatedLocationBlobMap{LocationBlobMap: locationBlobMap},
		gatedDecodes:    &gatedCoder{Coder: coder.JoinCoders(coder.NewProtoCoder[remoteexecution.ActionResult](), coder.NewSHA256SuffixCoder())},
		lock:            &sync.RWMutex{},
		digestKeyFormat: digestKeyFormat,
	}
	s.blobAccess = local.NewFlatBlobAccess(keyLocationMap, locationBlobMap, s.gatedPuts, digestKeyFormat, s.lock, "AC", nil, s.gatedDecodes)
	return s
}

func (s *refreshTestStore) key(blobDigest digest.Digest) local.Key {
	return local.NewKeyFromString(blobDigest.GetKey(s.digestKeyFormat))
}

func (s *refreshTestStore) location(t *testing.T, blobDigest digest.Digest) local.Location {
	s.lock.RLock()
	defer s.lock.RUnlock()
	location, err := s.keyLocationMap.Get(s.key(blobDigest), s.locationBlobMap)
	require.NoError(t, err)
	return location
}

func (s *refreshTestStore) needsRefresh(t *testing.T, blobDigest digest.Digest) bool {
	location := s.location(t, blobDigest)
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, needsRefresh := s.locationBlobMap.Get(location)
	return needsRefresh
}

// write stores data of a given size without adding a key-location map
// entry.
func (s *refreshTestStore) write(t *testing.T, sizeBytes int64) {
	s.lock.Lock()
	putWriter, err := s.locationBlobMap.Put(sizeBytes)
	s.lock.Unlock()
	require.NoError(t, err)
	putFinalizer := putWriter(make([]byte, sizeBytes))
	s.lock.Lock()
	_, err = putFinalizer()
	s.lock.Unlock()
	require.NoError(t, err)
}

// fillBlock writes one whole block. It causes exactly one new block to
// be created.
func (s *refreshTestStore) fillBlock(t *testing.T) {
	s.write(t, refreshTestBlockSize)
}

// age writes half blocks until the entry of a blob is stored in an
// "old" block. This leaves space in the newest block, so that the next
// write does not release the old block.
func (s *refreshTestStore) age(t *testing.T, blobDigest digest.Digest) {
	for i := 0; !s.needsRefresh(t, blobDigest); i++ {
		require.Less(t, i, 20)
		s.write(t, refreshTestBlockSize/2)
	}
}

// releaseAllBlocks writes whole blocks until every block that existed
// before the call has been released.
func (s *refreshTestStore) releaseAllBlocks(t *testing.T) {
	for i := 0; i < 4; i++ {
		s.fillBlock(t)
	}
}

func (s *refreshTestStore) refresh(ctx context.Context, blobDigest digest.Digest, viaFindMissing bool) error {
	if viaFindMissing {
		_, err := s.blobAccess.FindMissing(ctx, blobDigest.ToSingletonSet())
		return err
	}
	_, err := s.blobAccess.Get(ctx, blobDigest)
	return err
}

func (s *refreshTestStore) requireValue(t *testing.T, blobDigest digest.Digest, want *remoteexecution.ActionResult) {
	got, err := s.blobAccess.Get(context.Background(), blobDigest)
	require.NoError(t, err)
	require.True(t, proto.Equal(want, got), "got %v, want %v", got, want)
}

var (
	refreshTestDigest = digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256, "1111111111111111111111111111111111111111111111111111111111111111", 123)
	refreshTestOld    = &remoteexecution.ActionResult{ExitCode: 10, StdoutRaw: []byte("E0")}
	refreshTestNew    = &remoteexecution.ActionResult{ExitCode: 11, StdoutRaw: []byte("E1")}
)

func TestFlatBlobAccessRefreshDoesNotRevertPut(t *testing.T) {
	ctx := context.Background()
	d := refreshTestDigest

	for _, viaFindMissing := range []bool{false, true} {
		operation := "Get"
		if viaFindMissing {
			operation = "FindMissing"
		}
		newStore := func(t *testing.T) *refreshTestStore {
			s := newRefreshTestStore(t, 1024, 16, 64, digest.KeyWithoutInstance)
			require.NoError(t, s.blobAccess.Put(ctx, d, refreshTestOld))
			s.age(t, d)
			return s
		}

		// A Put() allocates, then the refresh allocates a newer
		// location. The Put() finalizes and returns, after which
		// the refresh finalizes.
		t.Run(operation+"/PutInFlightRefreshFinalizesLast", func(t *testing.T) {
			s := newStore(t)
			putGate, putAllocated := s.gatedPuts.arm()
			putDone := make(chan error, 1)
			go func() { putDone <- s.blobAccess.Put(ctx, d, refreshTestNew) }()
			<-putAllocated

			refreshGate, refreshAllocated := s.gatedPuts.arm()
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- s.refresh(ctx, d, viaFindMissing) }()
			select {
			case <-refreshAllocated:
				close(putGate)
				require.NoError(t, <-putDone)
				close(refreshGate)
				require.NoError(t, <-refreshDone)
			case err := <-refreshDone:
				// The refresh did not copy the blob.
				require.NoError(t, err)
				s.gatedPuts.disarm()
				close(putGate)
				require.NoError(t, <-putDone)
			}
			s.requireValue(t, d, refreshTestNew)
		})

		// The same, except that the refresh finalizes first. The
		// Put() is then ignored as older, but still succeeds.
		t.Run(operation+"/PutInFlightRefreshFinalizesFirst", func(t *testing.T) {
			s := newStore(t)
			putGate, putAllocated := s.gatedPuts.arm()
			putDone := make(chan error, 1)
			go func() { putDone <- s.blobAccess.Put(ctx, d, refreshTestNew) }()
			<-putAllocated

			require.NoError(t, s.refresh(ctx, d, viaFindMissing))
			close(putGate)
			require.NoError(t, <-putDone)
			s.requireValue(t, d, refreshTestNew)
		})

		// A Put() that allocates after the refresh gets a newer
		// location, so it wins regardless of the order in which
		// both finalize.
		t.Run(operation+"/PutStartsDuringCopy", func(t *testing.T) {
			s := newStore(t)
			refreshGate, refreshAllocated := s.gatedPuts.arm()
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- s.refresh(ctx, d, viaFindMissing) }()
			<-refreshAllocated

			require.NoError(t, s.blobAccess.Put(ctx, d, refreshTestNew))
			close(refreshGate)
			require.NoError(t, <-refreshDone)
			s.requireValue(t, d, refreshTestNew)
		})

		// The refresh is copying when a Put() starts and succeeds,
		// after which all blocks are released. The refresh's block
		// is released as well, so it fails and must not bring the
		// old value back.
		t.Run(operation+"/PutStartsDuringCopyAndIsReleased", func(t *testing.T) {
			s := newStore(t)
			refreshGate, refreshAllocated := s.gatedPuts.arm()
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- s.refresh(ctx, d, viaFindMissing) }()
			<-refreshAllocated

			require.NoError(t, s.blobAccess.Put(ctx, d, refreshTestNew))
			s.releaseAllBlocks(t)
			close(refreshGate)
			require.Error(t, <-refreshDone)
			_, err := s.blobAccess.Get(ctx, d)
			require.Equal(t, codes.NotFound, status.Code(err))
		})

		// Block indices in a Location are relative, so they shift
		// as blocks are released. The refresh reads the old value,
		// and before it re-checks the location, blocks rotate and a
		// Put() completes at an equal Location in a different
		// block. The refresh must notice that the location changed.
		t.Run(operation+"/LocationReusedAfterBlockRotation", func(t *testing.T) {
			s := newRefreshTestStore(t, 1024, 16, 64, digest.KeyWithoutInstance)
			require.NoError(t, s.blobAccess.Put(ctx, d, refreshTestOld))
			s.fillBlock(t)
			s.fillBlock(t)
			require.True(t, s.needsRefresh(t, d))
			oldLocation := s.location(t, d)

			decodeGate, decodeReached := s.gatedDecodes.arm()
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- s.refresh(ctx, d, viaFindMissing) }()
			<-decodeReached

			s.fillBlock(t)
			require.NoError(t, s.blobAccess.Put(ctx, d, refreshTestNew))
			s.fillBlock(t)
			s.fillBlock(t)
			require.Equal(t, oldLocation, s.location(t, d), "the new value should be stored at a Location equal to the old one")

			close(decodeGate)
			require.NoError(t, <-refreshDone)
			s.requireValue(t, d, refreshTestNew)
		})

		// The Put() that a refresh skipped for fails, because its
		// block is released before it finalizes. Later refreshes
		// of the same key must not be skipped.
		t.Run(operation+"/FailedPutDoesNotSuppressLaterRefreshes", func(t *testing.T) {
			s := newStore(t)
			putGate, putAllocated := s.gatedPuts.arm()
			putDone := make(chan error, 1)
			go func() { putDone <- s.blobAccess.Put(ctx, d, refreshTestNew) }()
			<-putAllocated

			refreshGate, refreshAllocated := s.gatedPuts.arm()
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- s.refresh(ctx, d, viaFindMissing) }()
			refreshCopying := false
			select {
			case <-refreshAllocated:
				refreshCopying = true
			case err := <-refreshDone:
				require.NoError(t, err)
				s.gatedPuts.disarm()
			}

			s.releaseAllBlocks(t)
			close(putGate)
			require.Error(t, <-putDone)
			if refreshCopying {
				close(refreshGate)
				require.Error(t, <-refreshDone)
			}

			require.NoError(t, s.blobAccess.Put(ctx, d, refreshTestOld))
			s.age(t, d)
			require.NoError(t, s.refresh(ctx, d, viaFindMissing))
			require.False(t, s.needsRefresh(t, d))
		})

		// A Put() that fails to allocate space must not cause
		// refreshes to be skipped.
		t.Run(operation+"/FailedAllocationDoesNotSuppressRefreshes", func(t *testing.T) {
			s := newStore(t)
			require.Error(t, s.blobAccess.Put(ctx, d, &remoteexecution.ActionResult{StdoutRaw: make([]byte, 2*refreshTestBlockSize)}))
			require.NoError(t, s.refresh(ctx, d, viaFindMissing))
			require.False(t, s.needsRefresh(t, d))
		})

		// The key-location map may drop the record of a newer value
		// while its block remains present. A refresh that started
		// before that value was written must not insert the old
		// value into the slot that became free.
		t.Run(operation+"/PutRecordDroppedFromKeyLocationMap", func(t *testing.T) {
			// With three records and two attempts, the digests
			// below probe slots target=[0,2], middle=[1,0] and
			// newest=[1].
			s := newRefreshTestStore(t, 3, 2, 2, digest.KeyWithInstance)
			makeDigest := func(n int) digest.Digest {
				return digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256, fmt.Sprintf("%064x", n), 123)
			}
			target, middle, newest := makeDigest(5), makeDigest(3), makeDigest(2)
			slot := func(blobDigest digest.Digest, attempt uint8) uint64 {
				return refreshTestRecordKeyHasher(&lossymap.RecordKey[local.Key]{Key: s.key(blobDigest), Attempt: attempt}) % 3
			}
			require.Equal(t, []uint64{0, 2, 1, 0, 1}, []uint64{slot(target, 0), slot(target, 1), slot(middle, 0), slot(middle, 1), slot(newest, 0)})

			require.NoError(t, s.blobAccess.Put(ctx, target, refreshTestOld))
			s.age(t, target)

			refreshGate, refreshAllocated := s.gatedPuts.arm()
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- s.refresh(ctx, target, viaFindMissing) }()
			<-refreshAllocated

			require.NoError(t, s.blobAccess.Put(ctx, target, refreshTestNew))
			newLocation := s.location(t, target)

			// newest displaces middle from slot 1, and middle
			// displaces target from slot 0. The iteration limit
			// is then reached, dropping target's record.
			require.NoError(t, s.blobAccess.Put(ctx, middle, &remoteexecution.ActionResult{ExitCode: 21}))
			require.NoError(t, s.blobAccess.Put(ctx, newest, &remoteexecution.ActionResult{ExitCode: 22}))
			s.lock.RLock()
			_, err := s.keyLocationMap.Get(s.key(target), s.locationBlobMap)
			getter, _ := s.locationBlobMap.Get(newLocation)
			_, _, readErr := getter(target)
			s.lock.RUnlock()
			require.Equal(t, codes.NotFound, status.Code(err))
			require.NoError(t, readErr, "the new value's block should still be present")

			close(refreshGate)
			require.NoError(t, <-refreshDone)
			_, err = s.blobAccess.Get(ctx, target)
			require.Equal(t, codes.NotFound, status.Code(err))
		})
	}
}
