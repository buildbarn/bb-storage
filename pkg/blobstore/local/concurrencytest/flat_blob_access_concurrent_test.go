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
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/lossymap"
)

type byteCoder struct{}

func (byteCoder) Encode(data []byte, d digest.Digest) ([]byte, error) { return data, nil }
func (byteCoder) Decode(data []byte, d digest.Digest) ([]byte, error) { return data, nil }

func newKeyLocationMap(entries int, seed uint64, name string) local.KeyLocationMap {
	return lossymap.NewHashMap(
		local.NewInMemoryLocationRecordArray(entries),
		func(k *local.LocationRecordKey) uint64 {
			h := seed
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
		},
		uint64(entries),
		func(a, b *local.Location) int {
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
		},
		16, 64, name,
	)
}

type silentErrorLogger struct{}

func (silentErrorLogger) Log(err error) {}

func makeDigest(seq uint64, sizeBytes int64) digest.Digest {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], seq)
	sum := sha256.Sum256(buf[:])
	return digest.MustNewDigest("bench", remoteexecution.DigestFunction_SHA256, hex.EncodeToString(sum[:]), sizeBytes)
}

// TestFlatBlobAccessConcurrentMixedTraffic exercises concurrent Put,
// Get and FindMissing against a persistent block list and a real
// key-location map. Allocation and finalization remain protected by
// the outer write lock while refresh copies may proceed concurrently.
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
	keyLocationMap := newKeyLocationMap(recordsCount, 0xdeadbeef, "racetest")

	var globalLock sync.RWMutex
	access, err := local.NewFlatBlobAccess(
		keyLocationMap,
		locationBlobMap,
		locationBlobMap,
		digest.KeyWithoutInstance,
		&globalLock,
		4,
		"racetest",
		capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
		byteCoder{},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Pre-populate so Get/FindMissing have something to hit.
	populated := make([]digest.Digest, 256)
	for i := range populated {
		d := makeDigest(uint64(i), blobSize)
		populated[i] = d
		data := make([]byte, blobSize)
		if err := access.Put(ctx, d, data); err != nil {
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
				if err := access.Put(ctx, d, data); err == nil {
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
				if _, err := access.Get(ctx, d); err != nil {
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
