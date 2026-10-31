package coder_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChunkMappingCoderEncodeDecodeRoundTrip(t *testing.T) {
	blobDigest := digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "00000000000000000000000000000000", 10)
	mapping, err := chunk.NewMappingFromDigests([]digest.Digest{helloDigest, worldDigest}, 10, false)
	require.NoError(t, err)

	c := coder.NewChunkMappingCoder( /* prevalidated = */ true)
	encoded, err := c.Encode(mapping, blobDigest)
	require.NoError(t, err)

	decoded, err := c.Decode(encoded, blobDigest)
	require.NoError(t, err)
	require.True(t, mapping.Equals(decoded))
	require.True(t, decoded.IsValidated())
}

func TestChunkMappingCoderDecodeRejectsMismatchedSize(t *testing.T) {
	blobDigest := digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "00000000000000000000000000000000", 11)
	mapping, err := chunk.NewMappingFromDigests([]digest.Digest{helloDigest, worldDigest}, 10, false)
	require.NoError(t, err)

	c := coder.NewChunkMappingCoder( /* prevalidated = */ true)
	encoded, err := c.Encode(mapping, blobDigest)
	require.NoError(t, err)

	_, err = c.Decode(encoded, blobDigest)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Chunk mapping does not compose to blob"), err)
}
