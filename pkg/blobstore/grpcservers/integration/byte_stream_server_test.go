package integration

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/require"
)

func TestByteStreamAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	blobData := makeRandomData(t, 2*maximumMessageSizeBytes, 0)
	digest := computeDigest(blobData)

	tests := []struct {
		name       string
		compressor remoteexecution.Compressor_Value
		data       []byte
	}{
		{name: "IDENTITY", compressor: remoteexecution.Compressor_IDENTITY, data: blobData},
		{name: "ZSTD", compressor: remoteexecution.Compressor_ZSTD, data: zstdEncode(blobData)},
	}

	t.Run("Write and read back binary data", func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				closer, _, _, _, bsClient := setupServers(t)
				defer closer()
				// Write blob.
				err := bytestreamWriteBlob(ctx, bsClient, test.data, digest, test.compressor)
				require.NoError(t, err, "Could not write blob")

				// Read back uncompressed.
				receivedData, err := bytestreamReadBlob(ctx, bsClient, digest, remoteexecution.Compressor_IDENTITY)
				require.NoError(t, err, "Could not read back uploaded data")
				require.Equal(t, blobData, receivedData, "Downloaded payload does not match uploaded data")

				// Read back compressed.
				receivedZstdData, err := bytestreamReadBlob(ctx, bsClient, digest, remoteexecution.Compressor_ZSTD)
				require.NoError(t, err, "Could not read back uploaded data")
				decompressedData, err := zstdDecode(receivedZstdData)
				require.NoError(t, err, "Failed to decompress ZSTD payload")
				require.Equal(t, blobData, decompressedData, "Downloaded compressed payload does not match uploaded data")
			})
		}
	})
}
