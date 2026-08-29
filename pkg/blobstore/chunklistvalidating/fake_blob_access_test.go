package chunklistvalidating_test

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeBlobAccess provides a thread-safe, in-memory BlobAccess for
// testing.
type fakeBlobAccess struct {
	blobstore.BlobAccess
	lock               sync.Mutex
	blobs              map[digest.Digest][]byte
	touches            map[digest.Digest]int // Tracks lifetime extensions
	chunkingParameters *remoteexecution.RepMaxCdcParams
}

func newFakeBlobAccess(chunkingParameters *remoteexecution.RepMaxCdcParams) *fakeBlobAccess {
	return &fakeBlobAccess{
		blobs:              make(map[digest.Digest][]byte),
		touches:            make(map[digest.Digest]int),
		chunkingParameters: chunkingParameters,
	}
}

func (f *fakeBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	f.lock.Lock()
	defer f.lock.Unlock()
	data, ok := f.blobs[d]
	if !ok {
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))
	}
	return buffer.NewValidatedBufferFromByteSlice(data)
}

func (f *fakeBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	data, err := b.ToByteSlice(100 * 1024 * 1024)
	if err != nil {
		return err
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.touches[d]++
	f.blobs[d] = data
	return nil
}

func (f *fakeBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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

func (f *fakeBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			RepMaxCdcParams: f.chunkingParameters,
		},
	}, nil
}

func (f *fakeBlobAccess) GetTouches(d digest.Digest) int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.touches[d]
}

func (f *fakeBlobAccess) ResetTouches() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.touches = make(map[digest.Digest]int)
}
