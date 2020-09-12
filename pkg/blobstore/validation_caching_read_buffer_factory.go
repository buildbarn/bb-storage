package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type validationCachingReadBufferFactory struct {
	base           ReadBufferFactory
	existenceCache *digest.ExistenceCache
}

// NewValidationCachingReadBufferFactory creates a decorator for
// ReadBufferFactory that disables data integrity checking for repeated
// requests for the same object. This may be a necessity for supporting
// efficient random access to blobs.
//
// Information on which blobs have been accessed previously is tracked
// in a digest.ExistenceCache. This means that an upper bound can be
// placed on the maximum amount of time integrity checking is disabled.
func NewValidationCachingReadBufferFactory(base ReadBufferFactory, existenceCache *digest.ExistenceCache) ReadBufferFactory {
	return &validationCachingReadBufferFactory{
		base:           base,
		existenceCache: existenceCache,
	}
}

func (f *validationCachingReadBufferFactory) isCached(blobDigest digest.Digest) bool {
	// ExistenceCache only supports set operations.
	return f.existenceCache.RemoveExisting(blobDigest.ToSingletonSet()).Empty()
}

func (f *validationCachingReadBufferFactory) maybeAddToCache(blobDigest digest.Digest, dataIsValid bool) {
	if dataIsValid {
		f.existenceCache.Add(blobDigest.ToSingletonSet())
	}
}

func (f *validationCachingReadBufferFactory) NewBufferFromByteSlice(blobDigest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	if f.isCached(blobDigest) {
		return buffer.NewValidatedBufferFromByteSlice(data)
	}
	return f.base.NewBufferFromByteSlice(
		blobDigest,
		data,
		func(dataIsValid bool) {
			f.maybeAddToCache(blobDigest, dataIsValid)
			dataIntegrityCallback(dataIsValid)
		})
}

func (f *validationCachingReadBufferFactory) NewBufferFromReader(blobDigest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	// TODO: There is no NewValidatedBufferFromReader() that we can
	// use to bypass checksum validation. It's also likely not that
	// useful to have, as these buffers don't provide random access
	// in the first place.
	return f.base.NewBufferFromReader(blobDigest, r, dataIntegrityCallback)
}

func (f *validationCachingReadBufferFactory) NewBufferFromReaderAt(blobDigest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	if f.isCached(blobDigest) {
		return buffer.NewValidatedBufferFromReaderAt(r, sizeBytes)
	}
	return f.base.NewBufferFromReaderAt(
		blobDigest,
		r,
		sizeBytes,
		func(dataIsValid bool) {
			f.maybeAddToCache(blobDigest, dataIsValid)
			dataIntegrityCallback(dataIsValid)
		})
}
