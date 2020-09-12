package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// LocationBlobGetter is a callback that is returned by
// LocationBlobMap.Get(). It can be used to obtain a Buffer that gives
// access to the data associated with the blob.
//
// Calls to LocationBlobMap.Put() and LocationBlobPutFinalizer
// invalidate any of the LocationBlobGetters returned by
// LocationBlobMap.Get(). Calling them is a programming mistake.
type LocationBlobGetter func(digest digest.Digest) buffer.Buffer

// LocationBlobPutWriter is a callback that is returned by
// LocationBlobMap.Put(). It can be used to store data corresponding to
// a blob in space that has been allocated. It is safe to call this
// function without holding any locks.
//
// This function blocks until all data contained in the Buffer has been
// processed or an error occurs. A LocationBlobPutFinalizer is returned
// that the caller must invoke while locked.
type LocationBlobPutWriter func(b buffer.Buffer) LocationBlobPutFinalizer

// LocationBlobPutFinalizer is returned by LocationBlobPutWriter after
// writing of data has finished. This function returns the location at
// which the blob was stored.
//
// The Location returned by this function must still be valid at the
// time it is returned. If the write takes such a long time that the
// space associated with the blob is already released, this function
// must fail.
type LocationBlobPutFinalizer func() (Location, error)

// LocationBlobMap implements a data store for blobs. Blobs can be
// reobtained by providing a location to Get() that is returned by
// Put(). Because not all locations are valid (and may not necessarily
// remain valid over time), all Locations provided to Get() must be
// validated using a BlockReferenceResolver.
//
// LocationBlobMap is only partially thread-safe. LocationBlobMap.Get()
// and LocationBlobGetter can be invoked in parallel (e.g., under a read
// lock), while LocationBlobMap.Put() and LocationBlobPutFinalizer must
// run exclusively (e.g., under a write lock). LocationBlobPutWriter is
// safe to call without holding any locks.
type LocationBlobMap interface {
	// Get information about a blob stored in the map.
	//
	// A LocationBlobGetter is returned that can be invoked to
	// instantiate a Buffer that gives access to the data associated
	// with the blob.
	//
	// In addition to that, it is returned whether the blob needs to
	// be refreshed. When true, there is a high probability that the
	// blob will disappear in the nearby future due to recycling of
	// storage space. The caller is advised to call Put() to
	// reupload the blob. Because Put() invalidates
	// LocationBlobGetters, this function must be called after the
	// LocationBlobGetters is invoked.
	Get(location Location) (LocationBlobGetter, bool)

	// Put a new blob to storage.
	//
	// This function returns a LocationBlobPutWriter, which must be
	// called afterwards to provide the data. Once finished, a
	// LocationBlobPutFinalizer must be invoked to release any
	// internal resources and obtain the blob's location.
	Put(sizeBytes int64) (LocationBlobPutWriter, error)
}
