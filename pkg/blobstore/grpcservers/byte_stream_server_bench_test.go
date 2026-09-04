package grpcservers_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"net"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// benchBlobAccess is a minimal in-memory BlobAccess. Get() wraps the
// stored bytes in a bytes.Reader and returns a Buffer constructed via
// NewValidatedBufferFromReaderAt, exercising the standard ChunkReader
// code path through the server.
type benchBlobAccess struct {
	capabilities.Provider
	blobs map[digest.Digest][]byte
}

func (b *benchBlobAccess) Get(_ context.Context, d digest.Digest) buffer.Buffer {
	data, ok := b.blobs[d]
	if !ok {
		panic("benchmark requested an unknown digest")
	}
	return buffer.NewValidatedBufferFromReaderAt(
		readAtCloser{bytes.NewReader(data)},
		int64(len(data)),
	)
}

func (benchBlobAccess) GetFromComposite(context.Context, digest.Digest, digest.Digest, slicing.BlobSlicer) buffer.Buffer {
	panic("not implemented")
}

func (benchBlobAccess) Put(context.Context, digest.Digest, buffer.Buffer) error {
	panic("not implemented")
}

func (benchBlobAccess) FindMissing(context.Context, digest.Set) (digest.Set, error) {
	panic("not implemented")
}

type readAtCloser struct{ *bytes.Reader }

func (readAtCloser) Close() error { return nil }

// makeBenchBlobs generates n random blobs of the given size, on the Go
// heap. Returns the BlobAccess together with the list of ByteStream
// resource names the client should request. The aggregate resident
// footprint is n*size; callers must size GOMEMLIMIT accordingly so the
// arena itself does not bind GC pacing.
func makeBenchBlobs(b *testing.B, n, size int) (blobstore.BlobAccess, []string) {
	b.Helper()
	blobs := make(map[digest.Digest][]byte, n)
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			b.Fatal(err)
		}
		sum := sha256.Sum256(data)
		hash := hex.EncodeToString(sum[:])
		d := digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256, hash, int64(size))
		blobs[d] = data
		names = append(names, d.GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY))
	}
	return &benchBlobAccess{blobs: blobs}, names
}

// startBenchServer spins up an in-process ByteStream server backed by
// ba and returns a connected client plus a cleanup function.
func startBenchServer(b *testing.B, ba blobstore.BlobAccess) (bytestream.ByteStreamClient, func()) {
	b.Helper()
	l := bufconn.Listen(1 << 20)
	// MaxConcurrentStreams lifts the HTTP/2 100-stream default
	// so parallelism*GOMAXPROCS in-flight RPCs are not queued at
	// the gRPC layer.
	server := grpc.NewServer(grpc.MaxConcurrentStreams(2048))
	// 64 KiB matches the production readChunkSize.
	bytestream.RegisterByteStreamServer(server, grpcservers.NewByteStreamServer(ba, 64*1024, bb_zstd.NewUnboundedPool(
		[]zstd.EOption{zstd.WithEncoderConcurrency(1)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(1)},
	)))
	go func() {
		_ = server.Serve(l)
	}()
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return l.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatal(err)
	}
	cleanup := func() {
		conn.Close()
		server.Stop()
	}
	return bytestream.NewByteStreamClient(conn), cleanup
}

// drainRead issues a Read RPC and discards every chunk. Returns the
// total number of bytes read.
func drainRead(ctx context.Context, client bytestream.ByteStreamClient, name string) (int64, error) {
	stream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: name})
	if err != nil {
		return 0, err
	}
	var total int64
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		total += int64(len(resp.Data))
	}
}

// gcMetrics wraps runtime/metrics to compute the GC CPU fraction and
// STW pause time accrued over a specific window. The underlying
// counters (/cpu/classes/gc/total:cpu-seconds, /cpu/classes/total:cpu-seconds,
// /sched/pauses/total/gc:seconds) are cumulative since process start,
// so a window measurement is the difference between two reads.
type gcMetrics struct {
	samples []metrics.Sample
}

func newGCMetrics() *gcMetrics {
	return &gcMetrics{samples: []metrics.Sample{
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/sched/pauses/total/gc:seconds"},
	}}
}

func (g *gcMetrics) read() (gcSec, totalSec, pauseSec float64) {
	metrics.Read(g.samples)
	return g.samples[0].Value.Float64(), g.samples[1].Value.Float64(), histogramSum(g.samples[2].Value.Float64Histogram())
}

// histogramSum estimates the total time recorded in a runtime/metrics
// histogram by weighting each bucket count by its midpoint. Buckets
// may have -Inf or +Inf edges; those degenerate to the finite neighbor
// so a single sample at an extreme bucket does not dominate the sum.
func histogramSum(h *metrics.Float64Histogram) float64 {
	var sum float64
	for i, c := range h.Counts {
		if c == 0 {
			continue
		}
		lo := h.Buckets[i]
		hi := h.Buckets[i+1]
		var mid float64
		switch {
		case math.IsInf(lo, -1) && math.IsInf(hi, +1):
			mid = 0
		case math.IsInf(lo, -1):
			mid = hi
		case math.IsInf(hi, +1):
			mid = lo
		default:
			mid = (lo + hi) / 2
		}
		sum += mid * float64(c)
	}
	return sum
}

// BenchmarkByteStreamReadUnderGCPressure exercises the server Read
// path in a regime where per-RPC allocation cost dominates: enough
// concurrent Read RPCs that fresh chunk buffers are allocated faster
// than the GC can reclaim them. The bb-storage instances that
// motivated this benchmark exhibited a positive feedback loop where
// rising GC CPU stretched out each RPC, which kept more in-flight
// buffers alive, which raised GC CPU further. The benchmark does not
// attempt to reproduce that runaway behavior -- it would not
// converge -- but it does measure per-RPC allocation cost in a regime
// where the Go runtime is forced to track it continuously.
//
// To get there the benchmark (a) pins GOMAXPROCS so the ratio of
// allocator throughput to available GC CPU is reproducible across
// hosts, (b) fans out enough in-flight RPCs that the allocator
// stays under sustained pressure, and (c) sets GOMEMLIMIT to a
// fixed multiple of the steady-state working set so the runtime
// has to assist GC on every allocation. None of these knobs model
// any particular deployment.
//
// Outside this regime -- large blobs where per-RPC wall time is
// dominated by moving bytes rather than per-chunk allocation, or
// generous memory budgets where GC has room to defer work -- the
// Read path's per-chunk allocation cost is negligible. The 16 MiB
// and 64 MiB sub-benchmarks cover that case and serve as a
// regression guard.
//
// Custom metrics, all computed over the benchmark window:
//
//   - %gc-cpu        : GC CPU (assist + dedicated + idle + pause)
//     as a fraction of total CPU time, from /cpu/classes/gc/total
//     and /cpu/classes/total.
//   - gc-pause-us/op : mean STW pause time per RPC, estimated from
//     the /sched/pauses/total/gc histogram by summing bucket
//     midpoints and dividing by b.N.
//   - gc-cycles/Kop  : completed GC cycles per thousand RPCs.
//   - heap-MiB-peak  : max HeapInuse observed by a 10ms sampler.
func BenchmarkByteStreamReadUnderGCPressure(b *testing.B) {
	const (
		gomaxprocs = 8
		// parallelism*GOMAXPROCS = 1024 in-flight RPCs. Lower
		// values do not push the 256 KiB sub-benchmark into the
		// GC-bound regime that exposes the per-chunk allocation
		// cost.
		parallelism = 128
		// numBlobs sets the working set's logical cardinality.
		// With parallelism*GOMAXPROCS goroutines drawing names
		// from a round-robin counter, a value much smaller than
		// the goroutine count means each goroutine cycles through
		// the same set of digests every few RPCs, but no single
		// digest is hammered to the exclusion of the others.
		numBlobs = 64
		// Each in-flight RPC briefly holds a chunk buffer plus
		// some gRPC stream state; transientBudget sizes
		// GOMEMLIMIT's headroom above the resident arena so that
		// only those transient allocations push against the
		// limit. The 3/2 ratio leaves just enough slack above the
		// steady-state working set that the GC has to run
		// continuously. At small blob sizes that is enough to
		// drive the runtime well past 50% GC CPU and the actual
		// heap blows through the limit because GC cannot keep up;
		// at large blob sizes per-RPC throughput is bounded by
		// data movement rather than allocation, GC has time to
		// catch up, and the heap stays near the limit.
		transientBudgetNum = 3
		transientBudgetDen = 2
		minTransientBudget = 64 << 20
	)

	// Pin GOMAXPROCS so the GC CPU fraction is reproducible. The
	// benchmark's GC pressure is a function of (allocator
	// throughput) / (available CPU); without pinning, the same
	// workload on a many-core host has so much idle CPU that GC
	// never saturates and the regime under test never materializes.
	prevProcs := runtime.GOMAXPROCS(gomaxprocs)
	defer runtime.GOMAXPROCS(prevProcs)

	sizes := []struct {
		name      string
		blobBytes int
	}{
		{"64KiB", 64 << 10},
		{"256KiB", 256 << 10},
		{"1MiB", 1 << 20},
		{"4MiB", 4 << 20},
		{"16MiB", 16 << 20},
		{"64MiB", 64 << 20},
	}
	for _, sz := range sizes {
		b.Run("BlobSize="+sz.name, func(b *testing.B) {
			// workingSet is an upper bound on bytes the server
			// holds across all in-flight RPCs at any moment.
			// The transient budget tracks it so GOMEMLIMIT
			// scales consistently with blob size.
			workingSet := gomaxprocs * parallelism * sz.blobBytes
			transientBudget := workingSet * transientBudgetNum / transientBudgetDen
			if transientBudget < minTransientBudget {
				transientBudget = minTransientBudget
			}
			// The arena (numBlobs * blobBytes) is resident
			// throughout the run, so include it in the memory
			// limit; otherwise the limit binds on the fixture
			// rather than on the transient allocations.
			memoryLimit := numBlobs*sz.blobBytes + transientBudget
			runByteStreamRead(b, sz.blobBytes, numBlobs, memoryLimit, parallelism)
		})
	}
}

// BenchmarkByteStreamRead is the companion to
// BenchmarkByteStreamReadUnderGCPressure that runs the same Read
// path without touching any runtime knobs: GOMAXPROCS, GOMEMLIMIT
// and parallelism are left at their defaults. Per-RPC wall time
// reflects whatever the host has on hand, and changes to the
// chunk-buffer lifecycle must not regress this case.
func BenchmarkByteStreamRead(b *testing.B) {
	const numBlobs = 64
	sizes := []struct {
		name      string
		blobBytes int
	}{
		{"64KiB", 64 << 10},
		{"256KiB", 256 << 10},
		{"1MiB", 1 << 20},
		{"4MiB", 4 << 20},
		{"16MiB", 16 << 20},
		{"64MiB", 64 << 20},
	}
	for _, sz := range sizes {
		b.Run("BlobSize="+sz.name, func(b *testing.B) {
			runByteStreamRead(b, sz.blobBytes, numBlobs, 0, 0)
		})
	}
}

// runByteStreamRead is the shared driver for the Read benchmarks. A
// non-positive memoryLimit skips GOMEMLIMIT, and a non-positive
// parallelism leaves b.RunParallel at its default (GOMAXPROCS).
func runByteStreamRead(b *testing.B, blobSize, numBlobs, memoryLimit, parallelism int) {
	if memoryLimit > 0 {
		prevLimit := debug.SetMemoryLimit(-1)
		debug.SetMemoryLimit(int64(memoryLimit))
		defer debug.SetMemoryLimit(prevLimit)
	}

	ba, names := makeBenchBlobs(b, numBlobs, blobSize)
	client, cleanup := startBenchServer(b, ba)
	defer cleanup()

	// Warm the connection and codec caches with a few serial RPCs
	// so the measured window does not include connection setup or
	// gRPC's BDP discovery for the flow-control window. (Allocator
	// caches scale with concurrency and are not meaningfully warmed
	// here, but the parallel phase reaches steady state quickly.)
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 30*time.Second)
	for i := 0; i < 32; i++ {
		if _, err := drainRead(warmCtx, client, names[i%numBlobs]); err != nil {
			b.Fatal(err)
		}
	}
	warmCancel()

	runtime.GC()
	gm := newGCMetrics()
	gcBefore, totalBefore, pauseBefore := gm.read()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	var peakHeap atomic.Uint64
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Millisecond)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				for {
					cur := peakHeap.Load()
					if ms.HeapInuse <= cur || peakHeap.CompareAndSwap(cur, ms.HeapInuse) {
						break
					}
				}
			}
		}
	}()

	b.SetBytes(int64(blobSize))
	b.ReportAllocs()
	if parallelism > 0 {
		// Drive parallelism*GOMAXPROCS in-flight RPCs so the
		// allocator stays under sustained pressure. b.RunParallel's
		// default p=GOMAXPROCS goroutines on a busy host leave
		// enough idle CPU per goroutine that GC never saturates.
		b.SetParallelism(parallelism)
	}
	var counter atomic.Uint64
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			i := counter.Add(1) - 1
			if _, err := drainRead(ctx, client, names[i%uint64(numBlobs)]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.StopTimer()
	close(done)
	gcAfter, totalAfter, pauseAfter := gm.read()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	ops := float64(b.N)
	gcDelta := gcAfter - gcBefore
	totalDelta := totalAfter - totalBefore
	pauseDelta := pauseAfter - pauseBefore
	if totalDelta > 0 {
		b.ReportMetric(gcDelta/totalDelta*100, "%gc-cpu")
	}
	b.ReportMetric(pauseDelta/ops*1e6, "gc-pause-us/op")
	b.ReportMetric(float64(msAfter.NumGC-msBefore.NumGC)/ops*1000, "gc-cycles/Kop")
	b.ReportMetric(float64(peakHeap.Load())/(1<<20), "heap-MiB-peak")
}
