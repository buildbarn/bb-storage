package integration

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestContentAddressableStorageAPI(t *testing.T) {
	blobData := makeRandomData(t, maximumMessageSizeBytes/2, 0)
	blobDigest := computeDigest(blobData)

	t.Run("GetCapabilities", func(t *testing.T) {
		closer, capabilitiesClient, _, _, _ := setupServers(t)
		defer closer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		capabilities, err := capabilitiesClient.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "allowed_instance",
		})
		require.NoError(t, err)

		cacheCaps := capabilities.GetCacheCapabilities()
		require.NotNil(t, cacheCaps)
		require.Contains(t, cacheCaps.DigestFunctions, remoteexecution.DigestFunction_SHA256)

		require.True(t, cacheCaps.SpliceBlobSupport)
		require.True(t, cacheCaps.SplitBlobSupport)

		chunkingParameters := cacheCaps.GetRepMaxCdcParams()
		require.NotNil(t, chunkingParameters)
		require.Equal(t, minChunkSizeBytes, int(chunkingParameters.MinChunkSizeBytes))
		require.Equal(t, 8*minChunkSizeBytes, int(chunkingParameters.HorizonSizeBytes))

		_, err = capabilitiesClient.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "forbidden_instance",
		})
		require.Error(t, err)
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.PermissionDenied, status.Code())
	})

	t.Run("UploadSplitFMBDownload", func(t *testing.T) {
		closer, _, casClient, _, _ := setupServers(t)
		defer closer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Upload a test blob.
		err := batchUploadBlob(ctx, casClient, blobData, blobDigest)
		require.NoError(t, err, "Failed to upload test data")

		// Ask server to split.
		chunkDigests, err := splitBlob(ctx, casClient, blobDigest)
		require.NoError(t, err, "Failed to split uploaded blob")

		// Check that all digests exist in CAS.
		allDigests := append(chunkDigests, blobDigest)
		missing, err := findMissingBlobs(ctx, casClient, allDigests)
		require.NoError(t, err, "Failed to find missing blobs")
		require.Empty(t, missing, "Blobs were unexpectedly missing")

		// Read back chunks and stitch them together.
		chunks, err := batchDownloadBlobs(ctx, casClient, chunkDigests)
		require.NoError(t, err, "Failed to download blobs")
		rebuiltBlob := make([]byte, 0, len(blobData))
		for _, chunk := range chunks {
			rebuiltBlob = append(rebuiltBlob, chunk...)
		}
		require.Equal(t, blobData, rebuiltBlob, "Blob did not stitch back into expected result")

		// Read back blob.
		blobs, err := batchDownloadBlobs(ctx, casClient, []digest.Digest{blobDigest})
		require.NoError(t, err)
		require.Len(t, blobs, 1)
		require.Equal(t, blobData, blobs[0])
	})
}

func TestRepMaxCDCSplitAndSpliceBehaviors(t *testing.T) {
	t.Run("RoundTripSplitThenSplice", func(t *testing.T) {
		closer, _, casClient, _, _ := setupServers(t)
		defer closer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		dataSize := (minChunkSizeBytes * 4) + 128
		data := makeRandomData(t, int(dataSize), 0)
		blobDigest := computeDigest(data)
		err := batchUploadBlob(ctx, casClient, data, blobDigest)
		require.NoError(t, err)

		digests, err := splitBlob(ctx, casClient, blobDigest)
		require.NoError(t, err, "Unexpected error when splitting blob.")

		err = spliceBlob(ctx, casClient, blobDigest, digests)
		require.NoError(t, err, "Unexpected error when splicing blob.")
	})

	t.Run("SpliceNonStandardChunkingThenSplit", func(t *testing.T) {
		closer, _, casClient, _, _ := setupServers(t)
		defer closer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		blobData := makeRandomData(t, int(minChunkSizeBytes*2), 0)

		chunk1 := blobData[:1]
		chunk2 := blobData[1:]

		digest1 := computeDigest(chunk1)
		digest2 := computeDigest(chunk2)
		blobDigest := computeDigest(blobData)
		chunkDigests := []digest.Digest{digest1, digest2}

		err := batchUploadBlob(ctx, casClient, chunk1, digest1)
		require.NoError(t, err, "Unexpected error when uploading chunk1.")

		err = batchUploadBlob(ctx, casClient, chunk2, digest2)
		require.NoError(t, err, "Unexpected error when uploading chunk2.")

		err = spliceBlob(ctx, casClient, blobDigest, chunkDigests)
		require.NoError(t, err, "Unexpected error when splicing chunks.")

		digests, err := splitBlob(ctx, casClient, blobDigest)
		require.NoError(t, err, "Unexpected error when splitting recently spliced blob.")
		require.NotEqual(t, chunkDigests, digests, "Split should not return non standard split result.")

		chunks, err := batchDownloadBlobs(ctx, casClient, digests)
		require.NoError(t, err, "Unexpected error when downloading chunks of split blob.")

		rebuiltBlob := make([]byte, 0, len(blobData))
		for _, chunk := range chunks {
			rebuiltBlob = append(rebuiltBlob, chunk...)
		}
		require.Equal(t, blobData, rebuiltBlob, "Blob did not stitch back into expected result")
	})

	t.Run("SpliceAlreadyExistsOrNoop", func(t *testing.T) {
		closer, _, casClient, _, _ := setupServers(t)
		defer closer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		blobData := []byte("This blob will be fully uploaded before we try to splice it.")
		blobDigest := computeDigest(blobData)
		err := batchUploadBlob(ctx, casClient, blobData, blobDigest)
		require.NoError(t, err, "Unexpected error when uploading blob.")

		chunk1 := blobData[:1]
		chunk2 := blobData[1:]
		digest1 := computeDigest(chunk1)
		digest2 := computeDigest(chunk2)
		chunkDigests := []digest.Digest{digest1, digest2}

		err = batchUploadBlob(ctx, casClient, chunk1, digest1)
		require.NoError(t, err, "Unexpected error when uploading chunk1.")

		err = batchUploadBlob(ctx, casClient, chunk2, digest2)
		require.NoError(t, err, "Unexpected error when uploading chunk2.")

		err = spliceBlob(ctx, casClient, blobDigest, chunkDigests)
		if err != nil {
			require.Equal(t, codes.AlreadyExists, status.Code(err), "Expected OK or ALREADY_EXISTS")
		}
	})

	t.Run("ValidationSpliceBlobRejections", func(t *testing.T) {
		closer, _, casClient, _, _ := setupServers(t)
		defer closer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		validData := makeRandomData(t, 512, 0)
		validDigest := computeDigest(validData)
		err := batchUploadBlob(ctx, casClient, validData, validDigest)
		require.NoError(t, err, "Unexpected error when uploading blob.")

		ghostDigest := computeDigest([]byte("I do not exist in storage"))

		tests := []struct {
			name         string
			blobDigest   digest.Digest
			chunkDigests []digest.Digest
			expectError  codes.Code
		}{
			{
				name:         "Missing Chunk",
				blobDigest:   ghostDigest,
				chunkDigests: []digest.Digest{ghostDigest},
				expectError:  codes.NotFound,
			},
			{
				name:         "Digest Mismatch",
				blobDigest:   computeDigest([]byte("Fake target")),
				chunkDigests: []digest.Digest{validDigest},
				expectError:  codes.InvalidArgument,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := spliceBlob(ctx, casClient, tc.blobDigest, tc.chunkDigests)
				require.Error(t, err)
				require.Equal(t, tc.expectError, status.Code(err), "Not the expected error: %s", err.Error())
			})
		}
	})

	t.Run("SpliceSplicedBlob", func(t *testing.T) {
		closer, _, casClient, _, _ := setupServers(t)
		defer closer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		blobData := makeRandomData(t, 2*minChunkSizeBytes+1, 0)
		blobDigest := computeDigest(blobData)
		chunk1 := blobData[:1]
		chunk2 := blobData[1:]
		digest1 := computeDigest(chunk1)
		digest2 := computeDigest(chunk2)
		chunkDigests := []digest.Digest{digest1, digest2}

		err := batchUploadBlob(ctx, casClient, chunk1, digest1)
		require.NoError(t, err, "Unexpected error when uploading chunk1.")

		err = batchUploadBlob(ctx, casClient, chunk2, digest2)
		require.NoError(t, err, "Unexpected error when uploading chunk2.")

		err = spliceBlob(ctx, casClient, blobDigest, chunkDigests)
		require.NoError(t, err, "Unexpected error when splicing blob.")
	})
}

func TestActionCacheAPI(t *testing.T) {
	smallData := []byte("small file contents")
	smallDigest := computeDigest(smallData)

	bigData := makeRandomData(t, int(minChunkSizeBytes*4), 0)
	bigDigest := computeDigest(bigData)

	tree := &remoteexecution.Tree{
		Root: &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{Name: "big.bin", Digest: bigDigest.GetProto(), IsExecutable: true},
				{Name: "small.txt", Digest: smallDigest.GetProto()},
			},
		},
	}
	treeData, err := proto.Marshal(tree)
	require.NoError(t, err)
	treeDigest := computeDigest(treeData)

	actionResult := &remoteexecution.ActionResult{
		OutputDirectories: []*remoteexecution.OutputDirectory{
			{Path: "build_output", TreeDigest: treeDigest.GetProto()},
		},
		ExitCode: 0,
	}

	actionDigest := computeDigest(makeRandomData(t, 128, 0))

	t.Run("CompleteActionResult", func(t *testing.T) {
		closer, _, casClient, acClient, _ := setupServers(t)
		defer closer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, batchUploadBlob(ctx, casClient, smallData, smallDigest))
		require.NoError(t, batchUploadBlob(ctx, casClient, bigData, bigDigest))
		require.NoError(t, batchUploadBlob(ctx, casClient, treeData, treeDigest))

		err := updateActionResult(ctx, acClient, actionDigest, actionResult)
		require.NoError(t, err)

		getResp, err := getActionResult(ctx, acClient, actionDigest)
		require.NoError(t, err)
		require.Equal(t, actionResult.ExitCode, getResp.ExitCode)
		require.Len(t, getResp.OutputDirectories, 1)
		require.Equal(t, treeDigest.GetProto().Hash, getResp.OutputDirectories[0].TreeDigest.Hash)
	})

	t.Run("IncompleteActionResult", func(t *testing.T) {
		closer, _, casClient, acClient, _ := setupServers(t)
		defer closer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Big data has not been uploaded in this test
		require.NoError(t, batchUploadBlob(ctx, casClient, smallData, smallDigest))
		require.NoError(t, batchUploadBlob(ctx, casClient, treeData, treeDigest))

		err = updateActionResult(ctx, acClient, actionDigest, actionResult)
		require.NoError(t, err, "Unexpected error updating action result")

		_, err := getActionResult(ctx, acClient, actionDigest)
		require.Error(t, err, "Incomplete action result should fail")
		require.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestAPIAuthorizationRejections(t *testing.T) {
	closer, capClient, casClient, acClient, bsClient := setupServers(t)
	defer closer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	forbiddenInstance := "forbidden_instance"
	data := []byte("top secret data")
	forbiddenDigest := computeDigestWithInstanceName(data, forbiddenInstance)
	chunkDigest1 := computeDigestWithInstanceName([]byte("top "), forbiddenInstance)
	chunkDigest2 := computeDigestWithInstanceName([]byte("secret data"), forbiddenInstance)

	dummyActionResult := &remoteexecution.ActionResult{
		ExitCode: 0,
	}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "GetCapabilities",
			call: func() error {
				_, err := capClient.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
					InstanceName: forbiddenInstance,
				})
				return err
			},
		},
		{
			name: "FindMissingBlobs",
			call: func() error {
				_, err := findMissingBlobs(ctx, casClient, []digest.Digest{forbiddenDigest})
				return err
			},
		},
		{
			name: "BatchUpdateBlobs",
			call: func() error {
				return batchUploadBlob(ctx, casClient, data, forbiddenDigest)
			},
		},
		{
			name: "BatchReadBlobs",
			call: func() error {
				_, err := batchDownloadBlobs(ctx, casClient, []digest.Digest{forbiddenDigest})
				return err
			},
		},
		{
			name: "SplitBlob",
			call: func() error {
				_, err := splitBlob(ctx, casClient, forbiddenDigest)
				return err
			},
		},
		{
			name: "SpliceBlob",
			call: func() error {
				return spliceBlob(ctx, casClient, forbiddenDigest, []digest.Digest{chunkDigest1, chunkDigest2})
			},
		},
		{
			name: "ByteStream Write",
			call: func() error {
				return bytestreamWriteBlob(ctx, bsClient, data, forbiddenDigest, remoteexecution.Compressor_IDENTITY)
			},
		},
		{
			name: "ByteStream Read",
			call: func() error {
				_, err := bytestreamReadBlob(ctx, bsClient, forbiddenDigest, remoteexecution.Compressor_IDENTITY)
				return err
			},
		},
		{
			name: "ActionCache UpdateActionResult",
			call: func() error {
				return updateActionResult(ctx, acClient, forbiddenDigest, dummyActionResult)
			},
		},
		{
			name: "ActionCache GetActionResult",
			call: func() error {
				_, err := getActionResult(ctx, acClient, forbiddenDigest)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.Error(t, err, "Expected API call to fail for forbidden instance")
			require.Equal(t, codes.PermissionDenied, status.Code(err), "Expected PermissionDenied, got %v", status.Code(err))
		})
	}
}
