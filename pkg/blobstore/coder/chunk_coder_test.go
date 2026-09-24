package coder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	helloDigest = digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	worldDigest = digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "f5a7924e621e84c9280a9a27e1bcb7f6", 5)
)

func TestChunkCoderEncodeDecodeRoundTrip(t *testing.T) {
	ctx := context.Background()
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	c := coder.NewChunkCoder(zstdPool)

	encoded, err := c.Encode(chunk.NewChunk(zstdPool, []byte("Hello")), helloDigest)
	require.NoError(t, err)

	decoded, err := c.Decode(encoded, helloDigest)
	require.NoError(t, err)
	decodedData, err := decoded.GetBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), decodedData)
}

func TestChunkCoderDecodeRejectsDigestMismatch(t *testing.T) {
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	c := coder.NewChunkCoder(zstdPool)

	// Payload of "World" decoded against the digest of "Hello".
	encoded, err := c.Encode(chunk.NewChunk(zstdPool, []byte("World")), worldDigest)
	require.NoError(t, err)
	_, err = c.Decode(encoded, helloDigest)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Digest mismatch, expected %s, got %s", helloDigest, worldDigest), err)
}
