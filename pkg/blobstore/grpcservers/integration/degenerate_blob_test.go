package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDegenerateUploadRejections(t *testing.T) {
	minChunkSizeBytes := 256 << 10
	socketPath := setupCluster(t, minChunkSizeBytes)
	_, casClient, _, bsClient := createClients(t, socketPath)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for _, size := range []struct {
		name     string
		blobSize int
	}{
		// Blobs smaller than twice the minimum chunk size are stored
		// as a single chunk and never obtain a chunk mapping.
		{name: "SingleChunk", blobSize: 2*minChunkSizeBytes - 128},
		{name: "MultiChunk", blobSize: 2*minChunkSizeBytes + 128},
	} {
		blobData := makeRandomData(t, size.blobSize, 0)
		blobDigest := computeDigest(blobData)

		tests := []struct {
			name        string
			digest      digest.Digest
			data        []byte
			expectError bool
		}{
			{
				name:        "DigestOversizeWithCorrectHash",
				digest:      digest.MustNewDigest("allowed_instance", remoteexecution.DigestFunction_SHA256, blobDigest.GetHashString(), int64(len(blobData)+128)),
				data:        blobData,
				expectError: true,
			},
			{
				name:        "DigestUndersizeWithCorrectHash",
				digest:      digest.MustNewDigest("allowed_instance", remoteexecution.DigestFunction_SHA256, blobDigest.GetHashString(), int64(len(blobData)-128)),
				data:        blobData,
				expectError: true,
			},
			{
				name:        "HashMismatch",
				digest:      digest.MustNewDigest("allowed_instance", remoteexecution.DigestFunction_MD5, "00000000000000000000000000000000", int64(len(blobData))),
				data:        blobData,
				expectError: true,
			},
		}

		for _, transport := range []struct {
			name   string
			upload func(ctx context.Context, data []byte, d digest.Digest, compressor remoteexecution.Compressor_Value) error
		}{
			{
				name: "BatchUpdateBlobs",
				upload: func(ctx context.Context, data []byte, d digest.Digest, compressor remoteexecution.Compressor_Value) error {
					return batchUploadBlob(ctx, casClient, data, d, compressor)
				},
			},
			{
				name: "ByteStream",
				upload: func(ctx context.Context, data []byte, d digest.Digest, compressor remoteexecution.Compressor_Value) error {
					return bytestreamWriteBlob(ctx, bsClient, data, d, compressor)
				},
			},
		} {
			for _, compressor := range []remoteexecution.Compressor_Value{remoteexecution.Compressor_IDENTITY, remoteexecution.Compressor_ZSTD} {
				for _, tc := range tests {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", transport.name, compressor.String(), size.name, tc.name), func(t *testing.T) {
						err := transport.upload(ctx, tc.data, tc.digest, compressor)
						require.Error(t, err)
						require.Equal(t, codes.InvalidArgument, status.Code(err), "Not the expected error: %s", err.Error())
					})
				}
			}
		}
	}
}
