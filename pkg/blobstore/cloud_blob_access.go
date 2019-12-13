package blobstore

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cloudBlobAccess struct {
	bucket      *blob.Bucket
	keyPrefix   string
	storageType StorageType
}

// NewCloudBlobAccess creates a BlobAccess that uses a cloud-based blob storage
// as a backend.
func NewCloudBlobAccess(bucket *blob.Bucket, keyPrefix string, storageType StorageType) BlobAccess {
	return &cloudBlobAccess{
		bucket:      bucket,
		keyPrefix:   keyPrefix,
		storageType: storageType,
	}
}

func (ba *cloudBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	key := ba.getKey(digest)
	result, err := ba.bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			err = status.Errorf(codes.NotFound, err.Error())
		}
		return buffer.NewBufferFromError(err)
	}
	return ba.storageType.NewBufferFromReader(
		digest,
		result,
		buffer.Reparable(digest, func() error {
			return ba.bucket.Delete(ctx, key)
		}))
}

func (ba *cloudBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	r := b.ToReader()
	defer r.Close()

	ctx, cancel := context.WithCancel(ctx)
	w, err := ba.bucket.NewWriter(ctx, ba.getKey(digest), nil)
	if err != nil {
		cancel()
		return err
	}
	// In case of an error (e.g. network failure), we cancel before closing to
	// request the write to be aborted.
	if _, err = io.Copy(w, r); err != nil {
		cancel()
		w.Close()
		return err
	}
	w.Close()
	cancel()
	return nil
}

func (ba *cloudBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	var missing []*util.Digest
	for _, digest := range digests {
		if exists, err := ba.bucket.Exists(ctx, ba.getKey(digest)); err != nil {
			return nil, err
		} else if !exists {
			missing = append(missing, digest)
		}
	}
	return missing, nil
}

func (ba *cloudBlobAccess) getKey(digest *util.Digest) string {
	return ba.keyPrefix + ba.storageType.GetDigestKey(digest)
}
