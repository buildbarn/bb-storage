package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type serverParams struct {
	disableCLS         bool
	socketPath         string
	upstreamSocketPath string
}

func escapeJSON(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func storageConfig(params serverParams) string {
	return fmt.Sprintf(`
local cls = %t;
local listenPath = '%s';
{
    grpcServers: [{
        listenPaths: [listenPath],
        authenticationPolicy: { allow: {} },
    }],
    maximumMessageSizeBytes: 4 * 1024 * 1024,
    contentAddressableStorage: {
        backend: {
            'local': {
                keyLocationMapInMemory: { entries: 1024 },
                keyLocationMapMaximumGetAttempts: 32,
                keyLocationMapMaximumPutAttempts: 64,
                oldBlocks: 1,
                currentBlocks: 1,
                newBlocks: 1,
                blocksInMemory: { blockSizeBytes: 1024 * 1024 },
            },
        },
        getAuthorizer: { allow: {} },
        putAuthorizer: { allow: {} },
        findMissingAuthorizer: { allow: {} },
    },
    chunkListStorage: if !cls then null else {
        backend: {
            'local': {
                keyLocationMapInMemory: { entries: 1024 },
                keyLocationMapMaximumGetAttempts: 32,
                keyLocationMapMaximumPutAttempts: 64,
                oldBlocks: 1,
                currentBlocks: 1,
                newBlocks: 1,
                blocksInMemory: { blockSizeBytes: 1024 * 1024 },
                chunkingParameters: {
                    minChunkSizeBytes: 256,
                    horizonSizeBytes: 8*256,
                }
            },
        },
        getAuthorizer: { allow: {} },
        putAuthorizer: { allow: {} },
        findMissingAuthorizer: { allow: {} },
    },
}
`, !params.disableCLS, escapeJSON(params.socketPath))
}

func frontendConfig(params serverParams) string {
	return fmt.Sprintf(`
local cls = %t;
local listenPath = '%s';
// unix://<path> doesn't work under Windows.
// https://github.com/grpc/grpc-go/issues/8675
local upstreamAddress = 'unix:%s';
{
    grpcServers: [{
        listenPaths: [listenPath],
        authenticationPolicy: { allow: {} },
    }],
    maximumMessageSizeBytes: 4 * 1024 * 1024,
    contentAddressableStorage: {
        backend: { grpc: { client: { address: upstreamAddress } } },
        getAuthorizer: { allow: {} },
        putAuthorizer: { allow: {} },
        findMissingAuthorizer: { allow: {} },
    },
    chunkListStorage: if !cls then null else {
        backend: { chunkListValidating: { backend: { grpc: { client: { address: upstreamAddress } } } } },
        getAuthorizer: { allow: {} },
        putAuthorizer: { allow: {} },
        findMissingAuthorizer: { allow: {} },
    },
}
`, !params.disableCLS, escapeJSON(params.socketPath), escapeJSON(params.upstreamSocketPath))
}

func writeConfigFile(name, content string) (file *os.File, err error) {
	if file, err = os.CreateTemp("", name); err != nil {
		return nil, err
	}
	if _, err = file.WriteString(content); err != nil {
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	return file, nil
}

func setupServer(t *testing.T, name, config string) func() {
	rf, err := runfiles.New()
	if err != nil {
		t.Fatalf("Failed to initialize runfiles: %v", err)
	}
	runfilePath := os.Getenv("BB_STORAGE_RUNFILE_PATH")
	require.NotEmpty(t, runfilePath, "BB_STORAGE_RUNFILE_PATH environment variable is not set")

	bbStoragePath, err := rf.Rlocation(runfilePath)
	require.NoError(t, err)

	configFile, err := writeConfigFile(name, config)
	require.NoError(t, err)

	cmd := exec.Command(bbStoragePath, configFile.Name())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	require.NoError(t, err)

	return func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.Remove(configFile.Name())
	}
}

func createSocketPath(t *testing.T) string {
	t.Helper()
	socketFile, err := os.CreateTemp("", "bb_*.sock")
	require.NoError(t, err)
	socketPath := socketFile.Name()
	socketFile.Close()
	err = os.Remove(socketPath)
	require.NoError(t, err)
	return socketPath
}

func setupServers(t *testing.T, storageParams, frontendParams serverParams) (func(), remoteexecution.CapabilitiesClient, remoteexecution.ContentAddressableStorageClient) {
	storageParams.socketPath = createSocketPath(t)
	closeStorage := setupServer(t, "storage", storageConfig(storageParams))
	require.Eventually(t, func() bool {
		_, err := os.Stat(storageParams.socketPath)
		return err == nil
	}, 1*time.Second, 10*time.Millisecond, "Storage server did not start.")

	frontendParams.socketPath = createSocketPath(t)
	frontendParams.upstreamSocketPath = storageParams.socketPath
	closeFrontend := setupServer(t, "frontend", frontendConfig(frontendParams))
	require.Eventually(t, func() bool {
		_, err := os.Stat(frontendParams.socketPath)
		return err == nil
	}, 1*time.Second, 10*time.Millisecond, "Frontend server did not start.")

	conn, err := grpc.NewClient(fmt.Sprintf("unix:%s", frontendParams.socketPath), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	return func() {
		closeStorage()
		closeFrontend()
		conn.Close()
		os.Remove(storageParams.socketPath)
		os.Remove(frontendParams.socketPath)
	}, remoteexecution.NewCapabilitiesClient(conn), remoteexecution.NewContentAddressableStorageClient(conn)
}

func TestChunkListValidatingCapabilities(t *testing.T) {
	tests := []struct {
		name           string
		storageParams  serverParams
		frontendParams serverParams
		expectSupport  bool
	}{
		{"Enabled In Both", serverParams{}, serverParams{}, true},
		{"Disabled in Storage", serverParams{disableCLS: true}, serverParams{}, false},
		{"Disabled in Frontend", serverParams{}, serverParams{disableCLS: true}, false},
		{"Disabled in Both", serverParams{disableCLS: true}, serverParams{disableCLS: true}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closer, capabilitiesClient, _ := setupServers(t, tc.storageParams, tc.frontendParams)
			defer closer()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			capabilities, err := capabilitiesClient.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
				InstanceName: "",
			})
			require.NoError(t, err)

			cacheCaps := capabilities.CacheCapabilities
			require.NotNil(t, cacheCaps)

			if tc.expectSupport {
				require.True(t, cacheCaps.SpliceBlobSupport)
				require.True(t, cacheCaps.SplitBlobSupport)

				chunkingParameters := cacheCaps.GetRepMaxCdcParams()
				require.NotNil(t, chunkingParameters)
				require.Equal(t, uint64(256), chunkingParameters.GetMinChunkSizeBytes())
				require.Equal(t, uint64(2048), chunkingParameters.GetHorizonSizeBytes())
			} else {
				require.False(t, cacheCaps.SpliceBlobSupport)
				require.False(t, cacheCaps.SplitBlobSupport)
				require.Nil(t, cacheCaps.GetRepMaxCdcParams())
			}
		})
	}
}

func computeDigest(data []byte) *remoteexecution.Digest {
	hash := sha256.Sum256(data)
	return &remoteexecution.Digest{
		Hash:      hex.EncodeToString(hash[:]),
		SizeBytes: int64(len(data)),
	}
}

func makeRandomData(t *testing.T, size int, seed int64) []byte {
	t.Helper()
	data := make([]byte, size)
	r := rand.New(rand.NewSource(seed))
	_, err := r.Read(data)
	require.NoError(t, err)
	return data
}

func uploadBlob(ctx context.Context, t *testing.T, cas remoteexecution.ContentAddressableStorageClient, data []byte) *remoteexecution.Digest {
	t.Helper()
	digest := computeDigest(data)
	req := &remoteexecution.BatchUpdateBlobsRequest{
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{Digest: digest, Data: data},
		},
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}
	res, err := cas.BatchUpdateBlobs(ctx, req)
	require.NoError(t, err)
	require.NotEmpty(t, res.Responses, "server returned empty responses array")
	status := res.Responses[0].GetStatus()
	require.Equal(t, int32(0), status.GetCode(), status.GetMessage())
	return digest
}

func findMissingBlobs(ctx context.Context, t *testing.T, cas remoteexecution.ContentAddressableStorageClient, digests []*remoteexecution.Digest) []*remoteexecution.Digest {
	t.Helper()
	req := &remoteexecution.FindMissingBlobsRequest{
		BlobDigests:    digests,
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}
	res, err := cas.FindMissingBlobs(ctx, req)
	require.NoError(t, err)
	return res.MissingBlobDigests
}

func TestRepMaxCDCSplitAndSpliceBehaviors(t *testing.T) {
	minChunkSize := int64(256)

	t.Run("RoundTripSplitThenSplice", func(t *testing.T) {
		closer, _, casClient := setupServers(t, serverParams{}, serverParams{})
		defer closer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		dataSize := (minChunkSize * 4) + 128
		data := makeRandomData(t, int(dataSize), 0)
		blobDigest := uploadBlob(ctx, t, casClient, data)

		splitReq := &remoteexecution.SplitBlobRequest{
			BlobDigest:       blobDigest,
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
		}
		splitRes, err := casClient.SplitBlob(ctx, splitReq)
		require.NoError(t, err)

		spliceReq := &remoteexecution.SpliceBlobRequest{
			BlobDigest:       blobDigest,
			ChunkDigests:     splitRes.ChunkDigests,
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
		}
		spliceRes, err := casClient.SpliceBlob(ctx, spliceReq)
		require.NoError(t, err)
		require.Equal(t, blobDigest.Hash, spliceRes.BlobDigest.Hash)
	})

	t.Run("SpliceNonStandardChunkingThenSplit", func(t *testing.T) {
		closer, _, casClient := setupServers(t, serverParams{}, serverParams{})
		defer closer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		blobData := makeRandomData(t, int(minChunkSize*2), 0)

		chunk1 := blobData[:1]
		chunk2 := blobData[1:]

		digest1 := uploadBlob(ctx, t, casClient, chunk1)
		digest2 := uploadBlob(ctx, t, casClient, chunk2)
		expectedDigest := computeDigest(blobData)

		spliceReq := &remoteexecution.SpliceBlobRequest{
			BlobDigest:       expectedDigest,
			ChunkDigests:     []*remoteexecution.Digest{digest1, digest2},
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
		}
		spliceRes, err := casClient.SpliceBlob(ctx, spliceReq)
		require.NoError(t, err)
		require.Equal(t, expectedDigest.Hash, spliceRes.BlobDigest.Hash)

		splitReq := &remoteexecution.SplitBlobRequest{
			BlobDigest:       expectedDigest,
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
		}
		splitRes, err := casClient.SplitBlob(ctx, splitReq)
		require.NoError(t, err)

		require.NotEmpty(t, splitRes.ChunkDigests)

		// Check that it didn't just echo our chunks back
		isEcho := len(splitRes.ChunkDigests) == 2 &&
			splitRes.ChunkDigests[0].Hash == digest1.Hash &&
			splitRes.ChunkDigests[1].Hash == digest2.Hash
		require.False(t, isEcho, "Server echoed non-standard chunks")

		var totalSize int64
		for _, c := range splitRes.ChunkDigests {
			totalSize += c.SizeBytes
		}
		require.Equal(t, expectedDigest.SizeBytes, totalSize)
	})

	t.Run("SpliceAlreadyExistsOrNoop", func(t *testing.T) {
		closer, _, casClient := setupServers(t, serverParams{}, serverParams{})
		defer closer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		blobData := append([]byte("This blob will be fully uploaded before we try to splice it."), makeRandomData(t, 16, 0)...)
		expectedDigest := uploadBlob(ctx, t, casClient, blobData)

		chunk1 := blobData[:10]
		chunk2 := blobData[10:]
		digest1 := uploadBlob(ctx, t, casClient, chunk1)
		digest2 := uploadBlob(ctx, t, casClient, chunk2)

		spliceReq := &remoteexecution.SpliceBlobRequest{
			BlobDigest:       expectedDigest,
			ChunkDigests:     []*remoteexecution.Digest{digest1, digest2},
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
		}

		spliceRes, err := casClient.SpliceBlob(ctx, spliceReq)

		if err != nil {
			require.Equal(t, codes.AlreadyExists, status.Code(err), "Expected OK or ALREADY_EXISTS")
		} else {
			require.Equal(t, expectedDigest.Hash, spliceRes.BlobDigest.Hash)
		}
	})

	t.Run("ValidationSpliceBlobRejections", func(t *testing.T) {
		closer, _, casClient := setupServers(t, serverParams{}, serverParams{})
		defer closer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		validData := makeRandomData(t, 512, 0)
		validDigest := uploadBlob(ctx, t, casClient, validData)
		ghostDigest := computeDigest([]byte("I do not exist"))

		tests := []struct {
			name        string
			req         *remoteexecution.SpliceBlobRequest
			expectError codes.Code
		}{
			{
				name: "Missing Chunk",
				req: &remoteexecution.SpliceBlobRequest{
					BlobDigest:   ghostDigest,
					ChunkDigests: []*remoteexecution.Digest{ghostDigest},
				},
				expectError: codes.NotFound,
			},
			{
				name: "Digest Mismatch",
				req: &remoteexecution.SpliceBlobRequest{
					BlobDigest:   computeDigest([]byte("Fake target")),
					ChunkDigests: []*remoteexecution.Digest{validDigest},
				},
				expectError: codes.InvalidArgument,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				tc.req.ChunkingFunction = remoteexecution.ChunkingFunction_REP_MAX_CDC
				tc.req.DigestFunction = remoteexecution.DigestFunction_SHA256

				_, err := casClient.SpliceBlob(ctx, tc.req)
				require.Error(t, err)
				require.Equal(t, tc.expectError, status.Code(err))
			})
		}
	})
}
