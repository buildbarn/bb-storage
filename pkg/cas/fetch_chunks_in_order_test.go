package cas_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newChunkDigests(n int) []digest.Digest {
	chunkDigests := make([]digest.Digest, 0, n)
	for i := range n {
		chunkDigests = append(chunkDigests, digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, fmt.Sprintf("%032x", i), 1))
	}
	return chunkDigests
}

func TestFetchChunksInOrderReadsAhead(t *testing.T) {
	chunkDigests := newChunkDigests(4)

	// fetch() only returns once all chunks are fetched at the same time.
	var started atomic.Int64
	allStarted := make(chan struct{})
	fetch := func(ctx context.Context, chunkDigest digest.Digest) (digest.Digest, error) {
		if started.Add(1) == int64(len(chunkDigests)) {
			close(allStarted)
		}
		select {
		case <-allStarted:
			return chunkDigest, nil
		case <-time.After(10 * time.Second):
			return digest.BadDigest, status.Error(codes.DeadlineExceeded, "Chunks are not fetched concurrently")
		}
	}

	var consumed []digest.Digest
	chunkSemaphore := semaphore.NewWeighted(3)
	require.NoError(t, cas.FetchChunksInOrder(context.Background(), chunkDigests, fetch, chunkSemaphore, func(chunkDigest digest.Digest) error {
		consumed = append(consumed, chunkDigest)
		return nil
	}))
	require.Equal(t, chunkDigests, consumed)
	require.True(t, chunkSemaphore.TryAcquire(3))
}

func TestFetchChunksInOrderWithoutFreeUnits(t *testing.T) {
	chunkDigests := newChunkDigests(4)

	// Another call holds all units. Chunks are fetched one at a time.
	chunkSemaphore := semaphore.NewWeighted(1)
	require.True(t, chunkSemaphore.TryAcquire(1))
	var fetching, overlapped atomic.Bool
	fetch := func(ctx context.Context, chunkDigest digest.Digest) (digest.Digest, error) {
		if fetching.Swap(true) {
			overlapped.Store(true)
		}
		defer fetching.Store(false)
		return chunkDigest, nil
	}

	var consumed []digest.Digest
	require.NoError(t, cas.FetchChunksInOrder(context.Background(), chunkDigests, fetch, chunkSemaphore, func(chunkDigest digest.Digest) error {
		consumed = append(consumed, chunkDigest)
		return nil
	}))
	require.Equal(t, chunkDigests, consumed)
	require.False(t, overlapped.Load())
}

func TestFetchChunksInOrderReleasesUnitsOnError(t *testing.T) {
	chunkDigests := newChunkDigests(8)

	fetch := func(ctx context.Context, chunkDigest digest.Digest) (digest.Digest, error) {
		if chunkDigest == chunkDigests[2] {
			return digest.BadDigest, status.Error(codes.Internal, "Disk on fire")
		}
		return chunkDigest, nil
	}

	var consumed []digest.Digest
	chunkSemaphore := semaphore.NewWeighted(4)
	err := cas.FetchChunksInOrder(context.Background(), chunkDigests, fetch, chunkSemaphore, func(chunkDigest digest.Digest) error {
		consumed = append(consumed, chunkDigest)
		return nil
	})
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)
	require.Equal(t, chunkDigests[:2], consumed)
	require.True(t, chunkSemaphore.TryAcquire(4))
}
