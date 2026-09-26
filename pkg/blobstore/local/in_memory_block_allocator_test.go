package local_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBlockAllocatorNewBlock(t *testing.T) {
	blockAllocator := local.NewInMemoryBlockAllocator(1024)

	block, location, err := blockAllocator.NewBlock()
	require.NoError(t, err)
	require.Nil(t, location)

	// Write an object into the block.
	offsetBytes, err := block.Put(11)([]byte("Hello world"))()
	require.NoError(t, err)
	require.Equal(t, int64(0), offsetBytes)

	// Extract it once again, using the right offset and size.
	data, err := block.Get(
		digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 456),
		0,
		11,
	)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)

	block.Release()
}

func TestInMemoryBlockAllocatorNewBlockAtLocation(t *testing.T) {
	blockAllocator := local.NewInMemoryBlockAllocator(1024)

	// InMemoryBlockAllocator provide no persistency, so
	// NewBlockAtLocation() should simply not function. There is no
	// way to get historical blocks back.
	_, found := blockAllocator.NewBlockAtLocation(nil, 700)
	require.False(t, found)

	_, found = blockAllocator.NewBlockAtLocation(&pb.BlockLocation{
		OffsetBytes: 1024,
		SizeBytes:   1024,
	}, 700)
	require.False(t, found)
}
