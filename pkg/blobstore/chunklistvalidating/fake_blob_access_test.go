package chunklistvalidating_test

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeBlobAccess provides a thread-safe, in-memory BlobAccess for
// testing.
type fakeBlobAccess[T any] struct {
	blobstore.BlobAccess[T]
	lock               sync.Mutex
	blobs              map[digest.Digest]T
	touches            map[digest.Digest]int // Tracks lifetime extensions
	chunkingParameters *remoteexecution.RepMaxCdcParams
}

func newFakeBlobAccess[T any](chunkingParameters *remoteexecution.RepMaxCdcParams) *fakeBlobAccess[T] {
	return &fakeBlobAccess[T]{
		blobs:              make(map[digest.Digest]T),
		touches:            make(map[digest.Digest]int),
		chunkingParameters: chunkingParameters,
	}
}

func (f *fakeBlobAccess[T]) Get(ctx context.Context, d digest.Digest) (T, error) {
	var zero T
	f.lock.Lock()
	defer f.lock.Unlock()
	data, ok := f.blobs[d]
	if !ok {
		return zero, status.Error(codes.NotFound, "Blob not found")
	}
	return data, nil
}

func (f *fakeBlobAccess[T]) Put(ctx context.Context, d digest.Digest, val T) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.touches[d]++
	f.blobs[d] = val
	return nil
}

func (f *fakeBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	missing := digest.NewSetBuilder(digests.Length())
	for _, d := range digests.Items() {
		if _, ok := f.blobs[d]; !ok {
			missing.Add(d)
		} else {
			f.touches[d]++
		}
	}
	return missing.Build(), nil
}

func (f *fakeBlobAccess[T]) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			RepMaxCdcParams: f.chunkingParameters,
		},
	}, nil
}

func (f *fakeBlobAccess[T]) GetTouches(d digest.Digest) int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.touches[d]
}

func (f *fakeBlobAccess[T]) ResetTouches() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.touches = make(map[digest.Digest]int)
}
