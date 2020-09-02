package blobstore

import (
	"context"
	"log"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/go-redis/redis/v8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RedisClient is an interface that contains the set of functions of the
// Redis library that is used by this package. This permits unit testing
// and uniform switching between clustered and single-node Redis.
type RedisClient interface {
	redis.Cmdable
	Process(ctx context.Context, cmd redis.Cmder) error
}

type redisBlobAccess struct {
	redisClient        RedisClient
	readBufferFactory  ReadBufferFactory
	digestKeyFormat    digest.KeyFormat
	keyTTL             time.Duration
	replicationCount   int64
	replicationTimeout int
}

// NewRedisBlobAccess creates a BlobAccess that uses Redis as its
// backing store.
func NewRedisBlobAccess(redisClient RedisClient, readBufferFactory ReadBufferFactory, digestKeyFormat digest.KeyFormat, keyTTL time.Duration, replicationCount int64, replicationTimeout time.Duration) BlobAccess {
	return &redisBlobAccess{
		redisClient:        redisClient,
		readBufferFactory:  readBufferFactory,
		digestKeyFormat:    digestKeyFormat,
		keyTTL:             keyTTL,
		replicationCount:   int64(replicationCount),
		replicationTimeout: int(replicationTimeout.Milliseconds()),
	}
}

func (ba *redisBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	if err := util.StatusFromContext(ctx); err != nil {
		return buffer.NewBufferFromError(err)
	}
	key := digest.GetKey(ba.digestKeyFormat)
	value, err := ba.redisClient.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.NotFound, "Blob not found"))
	} else if err != nil {
		return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Unavailable, "Failed to get blob"))
	}
	return ba.readBufferFactory.NewBufferFromByteSlice(
		digest,
		value,
		func(dataIsValid bool) {
			if !dataIsValid {
				if err := ba.redisClient.Del(ctx, key).Err(); err == nil {
					log.Printf("Blob %#v was malformed and has been deleted from Redis successfully", digest.String())
				} else {
					log.Printf("Blob %#v was malformed and could not be deleted from Redis: %s", digest.String(), err)
				}
			}
		})
}

func (ba *redisBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	if err := util.StatusFromContext(ctx); err != nil {
		b.Discard()
		return err
	}
	// Redis can only store values up to 512 MiB in size.
	value, err := b.ToByteSlice(512 * 1024 * 1024)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to put blob")
	}
	if err := ba.redisClient.Set(ctx, digest.GetKey(ba.digestKeyFormat), value, ba.keyTTL).Err(); err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to put blob")
	}
	return ba.waitIfReplicationEnabled(ctx)
}

func (ba *redisBlobAccess) waitIfReplicationEnabled(ctx context.Context) error {
	if ba.replicationCount == 0 {
		return nil
	}
	var command *redis.IntCmd
	if ba.replicationTimeout > 0 {
		command = redis.NewIntCmd(ctx, "wait", ba.replicationCount, ba.replicationTimeout)
	} else {
		command = redis.NewIntCmd(ctx, "wait", ba.replicationCount)
	}
	ba.redisClient.Process(ctx, command)
	replicatedCount, err := command.Result()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Error replicating blob")
	}
	if replicatedCount < ba.replicationCount {
		return status.Errorf(codes.Internal, "Replication not completed. Requested %d, actual %d", ba.replicationCount, replicatedCount)
	}
	return nil
}

func (ba *redisBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	if err := util.StatusFromContext(ctx); err != nil {
		return digest.EmptySet, err
	}
	if digests.Empty() {
		return digest.EmptySet, nil
	}

	// Execute "EXISTS" requests all in a single pipeline.
	pipeline := ba.redisClient.Pipeline()
	cmds := make([]*redis.IntCmd, 0, digests.Length())
	for _, digest := range digests.Items() {
		cmds = append(cmds, pipeline.Exists(ctx, digest.GetKey(ba.digestKeyFormat)))
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return digest.EmptySet, util.StatusWrapWithCode(err, codes.Unavailable, "Failed to find missing blobs")
	}

	missing := digest.NewSetBuilder()
	i := 0
	for _, digest := range digests.Items() {
		if cmds[i].Val() == 0 {
			missing.Add(digest)
		}
		i++
	}
	return missing.Build(), nil
}
