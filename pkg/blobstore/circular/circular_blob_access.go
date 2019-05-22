package circular

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"

	"go.opencensus.io/trace"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OffsetStore maps a digest to an offset within the data file. This is
// where the blob's contents may be found.
type OffsetStore interface {
	Get(digest *util.Digest, cursors Cursors) (uint64, int64, bool, error)
	Put(digest *util.Digest, offset uint64, length int64, cursors Cursors) error
}

// DataStore is where the data corresponding with a blob is stored. Data
// can be accessed by providing an offset within the data store and its
// length.
type DataStore interface {
	Put(r io.Reader, offset uint64) error
	Get(offset uint64, size int64) io.ReadCloser
}

// StateStore is where global metadata of the circular storage backend
// is stored, namely the read/write cursors where data is currently
// being stored in the data file.
type StateStore interface {
	GetCursors() Cursors
	Allocate(sizeBytes int64) (uint64, error)
	Invalidate(offset uint64, sizeBytes int64) error
}

type circularBlobAccess struct {
	// Fields that are constant or lockless.
	dataStore DataStore

	// Fields protected by the lock.
	lock        sync.Mutex
	offsetStore OffsetStore
	stateStore  StateStore
}

// NewCircularBlobAccess creates a new circular storage backend. Instead
// of writing data to storage directly, all three storage files are
// injected through separate interfaces.
func NewCircularBlobAccess(offsetStore OffsetStore, dataStore DataStore, stateStore StateStore) blobstore.BlobAccess {
	return &circularBlobAccess{
		offsetStore: offsetStore,
		dataStore:   dataStore,
		stateStore:  stateStore,
	}
}

func (ba *circularBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	ctx, span := trace.StartSpan(ctx, "circularBlobAccess.Get")
	defer span.End()
	ba.lock.Lock()
	span.Annotate(nil, "Lock obtained, calling GetCursors")
	cursors := ba.stateStore.GetCursors()
	offset, length, ok, err := ba.offsetStore.Get(digest, cursors)
	span.Annotate([]trace.Attribute{trace.Int64Attribute("offset", int64(offset)), trace.Int64Attribute("length", length), trace.BoolAttribute("object_found", ok)}, "offsetStore.Get completed")
	ba.lock.Unlock()
	if err != nil {
		return 0, nil, err
	} else if ok {
		span.Annotate(nil, "Obtaining body ReadCloser")
		return length, ba.dataStore.Get(offset, length), nil
	}
	return 0, nil, status.Errorf(codes.NotFound, "Blob not found")
}

func (ba *circularBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	defer r.Close()
	ctx, span := trace.StartSpan(ctx, "circularBlobAccess.Put")
	defer span.End()

	// Allocate space in the data store.
	ba.lock.Lock()
	span.Annotatef(nil, "Lock obtained, allocating %d bytes", sizeBytes)
	offset, err := ba.stateStore.Allocate(sizeBytes)
	ba.lock.Unlock()
	if err != nil {
		return err
	}
	span.Annotatef(nil, "Store allocated, offset %d", offset)

	// Write the data to storage.
	if err := ba.dataStore.Put(r, offset); err != nil {
		return err
	}

	span.Annotate(nil, "Obtaining lock")
	ba.lock.Lock()
	span.Annotate(nil, "Lock obtained, calling GetCursors")
	cursors := ba.stateStore.GetCursors()
	if cursors.Contains(offset, sizeBytes) {
		span.Annotate(nil, "Updating offsetStore")
		err = ba.offsetStore.Put(digest, offset, sizeBytes, cursors)
	} else {
		err = errors.New("Data became stale before write completed")
	}
	ba.lock.Unlock()
	return err
}

func (ba *circularBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	ba.lock.Lock()
	defer ba.lock.Unlock()

	cursors := ba.stateStore.GetCursors()
	if offset, length, ok, err := ba.offsetStore.Get(digest, cursors); err != nil {
		return err
	} else if ok {
		return ba.stateStore.Invalidate(offset, length)
	}
	return nil
}

func (ba *circularBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ba.lock.Lock()
	defer ba.lock.Unlock()

	cursors := ba.stateStore.GetCursors()
	var missingDigests []*util.Digest
	for _, digest := range digests {
		if _, _, ok, err := ba.offsetStore.Get(digest, cursors); err != nil {
			return nil, err
		} else if !ok {
			missingDigests = append(missingDigests, digest)
		}
	}
	return missingDigests, nil
}
