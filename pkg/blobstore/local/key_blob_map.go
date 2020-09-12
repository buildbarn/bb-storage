package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// KeyBlobGetter is a callback that is returned by
// KeyBlobMap.Get(). It can be used to obtain a Buffer that gives
// access to the data associated with the blob.
//
// Calls to KeyBlobMap.Put() and KeyBlobPutFinalizer invalidate any of
// the KeyBlobGetters returned by BlobStore.Get().
type KeyBlobGetter func(digest digest.Digest) buffer.Buffer

// KeyBlobPutWriter is a callback that is returned by KeyBlobMap.Put().
// It can be used to store data corresponding to a blob in space that
// has been allocated. It is safe to call this function without holding
// any locks.
//
// This function blocks until all data contained in the Buffer has been
// processed or an error occurs. A KeyBlobPutFinalizer is returned that
// the caller must invoke while locked.
type KeyBlobPutWriter func(b buffer.Buffer) KeyBlobPutFinalizer

// KeyBlobPutFinalizer is returned by KeyBlobPutWriter after writing of
// data has finished. The key of the blob must be provided, so that
// KeyBlobMap can register the blob and serve its contents going
// forward.
type KeyBlobPutFinalizer func(key Key) error

// KeyBlobMap implements a data type similar to a map[Key][]byte.
//
// KeyBlobMap is only partially thread-safe. KeyBlobMap.Get() and
// KeyBlobGetter can be invoked in parallel (e.g., under a read lock),
// while KeyBlobMap.Put() and KeyBlobPutFinalizer must run exclusively
// (e.g., under a write lock). KeyBlobPutWriter is safe to call without
// holding any locks.
type KeyBlobMap interface {
	// Get information about a blob stored in the map.
	//
	// Upon success, a KeyBlobGetter is returned that can be invoked
	// to instantiate a Buffer that gives access to the data
	// associated with the blob.
	//
	// In addition to that, the size of the blob is returned, and
	// whether the blob needs to be refreshed. When the latter is
	// true, there is a high probability that the blob will
	// disappear in the nearby future due to recycling of storage
	// space. The caller is advised to call Put() to reupload the
	// blob. Because Put() invalidates KeyBlobGetters, this function
	// must be called after the KeyBlobGetter is invoked.
	Get(key Key) (KeyBlobGetter, int64, bool, error)

	// Put a new blob to storage.
	//
	// This function returns a KeyBlobPutWriter, which must be
	// called afterwards to provide the data. Once finished, a
	// KeyBlobPutFinalizer must be invoked to associate the blob
	// with a Key.
	Put(sizeBytes int64) (KeyBlobPutWriter, error)
}
