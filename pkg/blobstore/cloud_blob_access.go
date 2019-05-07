package blobstore

import (
	"context"
	"io"

	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type cloudBlobAccess struct {
	bucket        *blob.Bucket
	keyPrefix     string
	blobKeyFormat util.DigestKeyFormat
}

func NewCloudUrlBlobAccess(url, keyPrefix string, keyFormat util.DigestKeyFormat) (*cloudBlobAccess, error) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, url)
	return &cloudBlobAccess{
		bucket:        bucket,
		keyPrefix:     keyPrefix,
		blobKeyFormat: keyFormat,
	}, err
}

func NewCloudAzureBlobAccess(pipeline pipeline.Pipeline, accountName azureblob.AccountName, containerName, keyPrefix string, keyFormat util.DigestKeyFormat) (*cloudBlobAccess, error) {
	ctx := context.Background()
	bucket, err := azureblob.OpenBucket(ctx, pipeline, accountName, containerName, nil)
	return &cloudBlobAccess{
		bucket: bucket,
		keyPrefix: keyPrefix,
		blobKeyFormat: keyFormat,
	}, err
}

func NewCloudGCSBlobAccess(client *gcp.HTTPClient, bucketName, keyprefix string, keyFormat util.DigestKeyFormat) (*cloudBlobAccess, error) {
	ctx := context.Background()
	bucket, err := gcsblob.OpenBucket(ctx, client, bucketName, nil)
	return &cloudBlobAccess{
		bucket:        bucket,
		keyPrefix:     keyprefix,
		blobKeyFormat: keyFormat,
	}, err
}

func NewCloudS3BlobAccess(sess *session.Session, bucketName, keyPrefix string, keyFormat util.DigestKeyFormat) (*cloudBlobAccess, error) {
	ctx := context.Background()
	bucket, err := s3blob.OpenBucket(ctx, sess, bucketName, nil)
	return &cloudBlobAccess{
		bucket:        bucket,
		keyPrefix:     keyPrefix,
		blobKeyFormat: keyFormat,
	}, err
}

func (ba *cloudBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	result, err := ba.bucket.NewReader(ctx, *ba.getKey(digest), nil)
	return result.Size(), result, err
}

func (ba *cloudBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	w, err := ba.bucket.NewWriter(ctx, *ba.getKey(digest), nil)
	defer w.Close()
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	return nil
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
