package cloud

import (
	"context"
	"log"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cloudBlobAccess struct {
	bucket            *blob.Bucket
	keyPrefix         string
	readBufferFactory blobstore.ReadBufferFactory
	digestKeyFormat   digest.KeyFormat
	beforeCopy        BeforeCopyFunc
}

// NewCloudBlobAccess creates a BlobAccess that uses a cloud-based blob storage
// as a backend.
func NewCloudBlobAccess(bucket *blob.Bucket, keyPrefix string, readBufferFactory blobstore.ReadBufferFactory, digestKeyFormat digest.KeyFormat, beforeCopy BeforeCopyFunc) blobstore.BlobAccess {
	return &cloudBlobAccess{
		bucket:            bucket,
		keyPrefix:         keyPrefix,
		readBufferFactory: readBufferFactory,
		digestKeyFormat:   digestKeyFormat,
		beforeCopy:        beforeCopy,
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

	b, t := buffer.WithBackgroundTask(ba.readBufferFactory.NewBufferFromReader(
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
		}))

	go func() {
		err := ba.touchBlob(ctx, key)
		if err != nil {
			err = util.StatusWrap(err, "Failed to refresh blob")
		}
		t.Finish(err)
	}()

	return b
}

func (ba *cloudBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	ctx, cancel := context.WithCancel(ctx)
	w, err := ba.bucket.NewWriter(ctx, ba.getKey(digest), nil)
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
		key := ba.getKey(blobDigest)
		err := ba.touchBlob(ctx, key)
		switch gcerrors.Code(err) {
		case gcerrors.OK:
			// Not missing
		case gcerrors.NotFound:
			// Missing
			missing.Add(blobDigest)
		default:
			return digest.EmptySet, err
		}
	}

	return missing.Build(), nil
}

func (ba *cloudBlobAccess) getKey(digest digest.Digest) string {
	return ba.keyPrefix + digest.GetKey(ba.digestKeyFormat)
}

func (ba *cloudBlobAccess) touchBlob(ctx context.Context, key string) error {
	// Touch the object to update its modification time, so that cloud expiration policies will be LRU
	return ba.bucket.Copy(ctx, key, key, &blob.CopyOptions{
		BeforeCopy: ba.beforeCopy,
	})
}
