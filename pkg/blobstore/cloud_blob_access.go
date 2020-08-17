package blobstore

import (
	"context"
	"log"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cloudBlobAccess struct {
	bucket            *blob.Bucket
	keyPrefix         string
	readBufferFactory ReadBufferFactory
	digestKeyFormat   digest.KeyFormat
	partSize          int64
}

// NewCloudBlobAccess creates a BlobAccess that uses a cloud-based blob storage
// as a backend.
func NewCloudBlobAccess(bucket *blob.Bucket, keyPrefix string, readBufferFactory ReadBufferFactory, digestKeyFormat digest.KeyFormat, partSize int64) BlobAccess {
	return &cloudBlobAccess{
		bucket:            bucket,
		keyPrefix:         keyPrefix,
		readBufferFactory: readBufferFactory,
		digestKeyFormat:   digestKeyFormat,
		partSize:          partSize,
	}
}

func (ba *cloudBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	key := ba.getKey(digest)
	result, err := ba.bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			err = status.Errorf(codes.NotFound, err.Error())
		}
		return buffer.NewBufferFromError(err)
	}
	return ba.readBufferFactory.NewBufferFromReader(
		digest,
		result,
		func(dataIsValid bool) {
			if !dataIsValid {
				if err := ba.bucket.Delete(ctx, key); err == nil {
					log.Printf("Blob %#v was malformed and has been deleted from its bucket successfully", digest.String())
				} else {
					log.Printf("Blob %#v was malformed and could not be deleted from its bucket: %s", digest.String(), err)
				}
			}
		})
}

func (ba *cloudBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	ctx, cancel := context.WithCancel(ctx)
	w, err := ba.bucket.NewWriter(ctx, ba.getKey(digest), &blob.WriterOptions{
		BufferSize: int(ba.partSize),
	})
	if err != nil {
		cancel()
		b.Discard()
		return err
	}
	// In case of an error (e.g. network failure), we cancel before closing to
	// request the write to be aborted.
	if err = b.IntoWriter(w); err != nil {
		cancel()
		w.Close()
		return err
	}
	w.Close()
	cancel()
	return nil
}

func (ba *cloudBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missing := digest.NewSetBuilder()
	for _, blobDigest := range digests.Items() {
		if exists, err := ba.bucket.Exists(ctx, ba.getKey(blobDigest)); err != nil {
			return digest.EmptySet, err
		} else if !exists {
			missing.Add(blobDigest)
		}
	}
	return missing.Build(), nil
}

func (ba *cloudBlobAccess) getKey(digest digest.Digest) string {
	return ba.keyPrefix + digest.GetKey(ba.digestKeyFormat)
}
