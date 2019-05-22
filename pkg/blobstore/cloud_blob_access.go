package blobstore

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/util"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cloudBlobAccess struct {
	bucket        *blob.Bucket
	keyPrefix     string
	blobKeyFormat util.DigestKeyFormat
}

func NewCloudBlobAccess(bucket *blob.Bucket, keyPrefix string, keyFormat util.DigestKeyFormat) *cloudBlobAccess {
	return &cloudBlobAccess{
		bucket:        bucket,
		keyPrefix:     keyPrefix,
		blobKeyFormat: keyFormat,
	}
}

func (ba *cloudBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	result, err := ba.bucket.NewReader(ctx, *ba.getKey(digest), nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			err = status.Errorf(codes.NotFound, err.Error())
		}
		return 0, nil, err
	}
	return result.Size(), result, err
}

func (ba *cloudBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	w, err := ba.bucket.NewWriter(ctx, *ba.getKey(digest), nil)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	return err
}

func (ba *cloudBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	return ba.bucket.Delete(ctx, *ba.getKey(digest))
}

func (ba *cloudBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	var missing []*util.Digest
	for _, digest := range digests {
		if exists, err := ba.bucket.Exists(ctx, *ba.getKey(digest)); err != nil {
			return nil, err
		} else if !exists {
			missing = append(missing, digest)
		}
	}
	return missing, nil
}

func (ba *cloudBlobAccess) getKey(digest *util.Digest) *string {
	s := ba.keyPrefix + digest.GetKey(ba.blobKeyFormat)
	return &s
}
