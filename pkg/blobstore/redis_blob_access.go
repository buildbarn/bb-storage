package blobstore

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/go-redis/redis"

	"google.golang.org/grpc/codes"
)

type redisBlobAccess struct {
	redisClient   *redis.Client
	blobKeyFormat util.DigestKeyFormat
}

// NewRedisBlobAccess creates a BlobAccess that uses Redis as its
// backing store.
func NewRedisBlobAccess(redisClient *redis.Client, blobKeyFormat util.DigestKeyFormat) BlobAccess {
	return &redisBlobAccess{
		redisClient:   redisClient,
		blobKeyFormat: blobKeyFormat,
	}
}

func (ba *redisBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	value, err := ba.redisClient.Get(digest.GetKey(ba.blobKeyFormat)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return 0, nil, util.StatusWrapWithCode(err, codes.NotFound, "Failed to get blob")
		}
		return 0, nil, util.StatusWrapWithCode(err, codes.Unavailable, "Failed to get blob")
	}
	return int64(len(value)), ioutil.NopCloser(bytes.NewBuffer(value)), nil
}

func (ba *redisBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	if err := ctx.Err(); err != nil {
		r.Close()
		return err
	}
	value, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to put blob")
	}
	return ba.redisClient.Set(digest.GetKey(ba.blobKeyFormat), value, 0).Err()
}

func (ba *redisBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	if err := ba.redisClient.Del(digest.GetKey(ba.blobKeyFormat)).Err(); err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to delete blob")
	}
	return nil
}

func (ba *redisBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(digests) == 0 {
		return nil, nil
	}

	// Execute "EXISTS" requests all in a single pipeline.
	pipeline := ba.redisClient.Pipeline()
	var cmds []*redis.IntCmd
	for _, digest := range digests {
		cmds = append(cmds, pipeline.Exists(digest.GetKey(ba.blobKeyFormat)))
	}
	if _, err := pipeline.Exec(); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unavailable, "Failed to find missing blobs")
	}

	var missing []*util.Digest
	for i, cmd := range cmds {
		if cmd.Val() == 0 {
			missing = append(missing, digests[i])
		}
	}
	return missing, nil
}
