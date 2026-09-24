package concurrencytest_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type silentErrorLogger struct{}

func (silentErrorLogger) Log(err error) {}

func makeDigest(seq uint64, sizeBytes int64) digest.Digest {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], seq)
	sum := sha256.Sum256(buf[:])
	return digest.MustNewDigest("bench", remoteexecution.DigestFunction_SHA256, hex.EncodeToString(sum[:]), sizeBytes)
}

// TestFlatBlobAccessConcurrentMixedTraffic is the higher-level race
// test that complements TestPersistentBlockListConcurrentFinalizeAndResolve.
//
// It wires a real FlatBlobAccess with PersistentBlockList +
// OldCurrentNewLocationBlobMap + hashingKeyLocationMap and drives
// concurrent Put + Get + FindMissing traffic under the race detector
// for two seconds. Without the internal synchronisation added to
// PersistentBlockList / hashingKeyLocationMap the detector trips; with
// it the test passes cleanly.
//
// This catches scenarios that the isolated PersistentBlockList test
// can't — in particular: FlatBlobAccess.Put's alloc+copy+finalize
// pipeline interleaving with concurrent Gets going through klm.Get +
// resolver + locationBlobMap.Get, and refresh paths triggered by old
// blocks rotating out.
//
// Run under the race detector as:
//
//	bazel test //pkg/blobstore/local/concurrencytest:concurrencytest_test \
//	    --host_platform=@platforms//host \
//	    --@rules_go//go/config:race \
//	    --test_filter=TestFlatBlobAccessConcurrentMixedTraffic
func TestFlatBlobAccessConcurrentMixedTraffic(t *testing.T) {
	const (
		blobSize       = 256
		blockSizeBytes = 1 << 20 // 1 MiB
		oldBlocks      = 2
		currentBlocks  = 6
		newBlocks      = 2
		klmEntries     = 16384
		duration       = 2 * time.Second
	)

	blockAllocator := local.NewInMemoryBlockAllocator(int(blockSizeBytes))
	blockList, _ := local.NewPersistentBlockList(blockAllocator, 0, nil)
	growthPolicy := local.NewImmutableBlockListGrowthPolicy(currentBlocks, newBlocks)

	locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
		blockList,
		growthPolicy,
		silentErrorLogger{},
		"racetest",
		blockSizeBytes,
		oldBlocks,
		newBlocks,
		0,
	)

	// Pick a record-count that's prime (KLM convention).
	recordsCount := klmEntries
	for !isPrime(recordsCount) {
		recordsCount++
	}
	recordArray := local.NewInMemoryLocationRecordArray(recordsCount, locationBlobMap)
	keyLocationMap := local.NewHashingKeyLocationMap(
		recordArray,
		recordsCount,
		0xdeadbeef,
		16,
		64,
		"racetest",
	)

	var globalLock sync.RWMutex
	access := local.NewFlatBlobAccess(
		keyLocationMap,
		locationBlobMap,
		digest.KeyWithoutInstance,
		&globalLock,
		0,
		"racetest",
		capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
	)

	ctx := context.Background()

	// Pre-populate so Get/FindMissing have something to hit.
	populated := make([]digest.Digest, 256)
	for i := range populated {
		d := makeDigest(uint64(i), blobSize)
		populated[i] = d
		data := make([]byte, blobSize)
		if err := access.Put(ctx, d, buffer.NewValidatedBufferFromByteSlice(data)); err != nil {
			t.Fatalf("populate Put[%d]: %v", i, err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var (
		puts        atomic.Uint64
		gets        atomic.Uint64
		getsMissing atomic.Uint64
		finds       atomic.Uint64
	)

	const writers, readers, finders = 8, 8, 4

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			seq := uint64(1_000_000) + uint64(id)*1_000_000
			data := make([]byte, blobSize)
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq++
				d := makeDigest(seq, blobSize)
				if err := access.Put(ctx, d, buffer.NewValidatedBufferFromByteSlice(data)); err == nil {
					puts.Add(1)
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(uint64(id), 99))
			for {
				select {
				case <-stop:
					return
				default:
				}
				d := populated[rnd.IntN(len(populated))]
				buf := access.Get(ctx, d)
				if _, err := buf.ToByteSlice(blobSize); err != nil {
					getsMissing.Add(1)
				} else {
					gets.Add(1)
				}
			}
		}(r)
	}

	for f := 0; f < finders; f++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(uint64(id), 7))
			for {
				select {
				case <-stop:
					return
				default:
				}
				b := digest.NewSetBuilder(32)
				for i := 0; i < 32; i++ {
					b.Add(populated[rnd.IntN(len(populated))])
				}
				if _, err := access.FindMissing(ctx, b.Build()); err == nil {
					finds.Add(1)
				}
			}
		}(f)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// gets must be non-zero on its own: getsMissing counts FAILED reads,
	// so a guard on the sum would accept a run in which every read
	// errored and the read path was never really exercised.
	if puts.Load() == 0 || gets.Load() == 0 || finds.Load() == 0 {
		t.Errorf("workload did not exercise all paths: puts=%d gets=%d gets_missing=%d finds=%d",
			puts.Load(), gets.Load(), getsMissing.Load(), finds.Load())
	}
}

func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	for i := 2; i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}
