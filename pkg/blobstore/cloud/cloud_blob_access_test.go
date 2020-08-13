package cloud_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cloud"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gocloud.dev/blob/memblob"
)

// These tests use memblob as a fake bucket. blob.Bucket isn't
// reasonably mockable, its methods return struct types which can't be
// constructed.

func TestCloudBlobAccessGet(t *testing.T) {
	ctx := context.Background()

	hello := []byte("Hello world")
	blobDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	bucketKey := blobDigest.GetKey(digest.KeyWithoutInstance)

	t.Run("Success", func(t *testing.T) {
		bucket := memblob.OpenBucket(nil)
		defer bucket.Close()
		require.NoError(t, bucket.WriteAll(ctx, bucketKey, hello, nil))
		blobAccess := cloud.NewCloudBlobAccess(bucket, "", blobstore.CASReadBufferFactory, digest.KeyWithoutInstance, nil)

		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("NotFound", func(t *testing.T) {
		bucket := memblob.OpenBucket(nil)
		defer bucket.Close()
		blobAccess := cloud.NewCloudBlobAccess(bucket, "", blobstore.CASReadBufferFactory, digest.KeyWithoutInstance, nil)

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("Repair", func(t *testing.T) {
		bucket := memblob.OpenBucket(nil)
		defer bucket.Close()
		require.NoError(t, bucket.WriteAll(ctx, bucketKey, []byte("HELLO WORLD"), nil))
		blobAccess := cloud.NewCloudBlobAccess(bucket, "", blobstore.CASReadBufferFactory, digest.KeyWithoutInstance, nil)

		// Precondition to validate the above code: key exists before we run .Get
		existsBefore, err := bucket.Exists(ctx, bucketKey)
		require.NoError(t, err)
		require.Equal(t, existsBefore, true)

		_, err = blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Buffer has checksum 787ec76dcafd20c1908eb0936a12f91edd105ab5cd7ecc2b1ae2032648345dff, while 64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c was expected"), err)

		// Postcondition: repair deletes the incorrect object
		existsAfter, err := bucket.Exists(ctx, bucketKey)
		require.NoError(t, err)
		require.Equal(t, existsAfter, false)
	})
}

func TestCloudBlobAccessPut(t *testing.T) {
	ctx := context.Background()

	hello := []byte("Hello world")
	blobDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	bucketKey := blobDigest.GetKey(digest.KeyWithoutInstance)

	t.Run("Success", func(t *testing.T) {
		bucket := memblob.OpenBucket(nil)
		defer bucket.Close()
		blobAccess := cloud.NewCloudBlobAccess(bucket, "", blobstore.CASReadBufferFactory, digest.KeyWithoutInstance, nil)

		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(hello)))
		data, err := bucket.ReadAll(ctx, bucketKey)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})
}

func TestCloudBlobAccessFindMissing(t *testing.T) {
	ctx := context.Background()

	hello := []byte("Hello world")
	helloDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	missingDigest := digest.MustNewDigest("default", "deadbeef00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	allDigests := digest.NewSetBuilder().Add(helloDigest).Add(missingDigest).Build()
	helloKey := helloDigest.GetKey(digest.KeyWithoutInstance)

	t.Run("HelloFound", func(t *testing.T) {
		bucket := memblob.OpenBucket(nil)
		defer bucket.Close()
		require.NoError(t, bucket.WriteAll(ctx, helloKey, hello, nil))
		blobAccess := cloud.NewCloudBlobAccess(bucket, "", blobstore.CASReadBufferFactory, digest.KeyWithoutInstance, nil)

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, digest.NewSetBuilder().Add(missingDigest).Build(), missing)
	})
}
