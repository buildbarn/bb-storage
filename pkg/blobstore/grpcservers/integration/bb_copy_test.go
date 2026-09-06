package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func standaloneCASConfig(listenSocketPath string, minChunkSizeBytes int) string {
	return fmt.Sprintf(`
local listenPath = '%s';
local maximumMessageSizeBytes = %d;
local minChunkSizeBytes = %d;

local inMemoryStorage = {
	keyLocationMapInMemory: { entries: 1024 * 1024 },
	keyLocationMapMaximumGetAttempts: 32,
	keyLocationMapMaximumPutAttempts: 64,
	oldBlocks: 1,
	currentBlocks: 1,
	newBlocks: 1,
	blocksInMemory: { blockSizeBytes: 32 * 1024 * 1024 },
};

{
	grpcServers: [{
		listenPaths: [listenPath],
		authenticationPolicy: { allow: {} },
	}],
	maximumMessageSizeBytes: maximumMessageSizeBytes,
	contentAddressableStorageServer: {
		contentAddressableStorage: {
			chunkStorage: {
				'local': inMemoryStorage + { 
					chunkingParameters: { 
						minChunkSizeBytes: minChunkSizeBytes, 
						horizonSizeBytes: 8 * minChunkSizeBytes 
					} 
				},
			},
			chunkListStorage: {
				chunkListValidating: {
					backend: { 'local': inMemoryStorage }
				}
			},
		},
		getAuthorizer: { allow: {} },
		putAuthorizer: { allow: {} },
		findMissingAuthorizer: { allow: {} },
	},
}
`, escapeJSON(listenSocketPath), maximumMessageSizeBytes, minChunkSizeBytes)
}

func setupStandaloneCAS(t *testing.T, minChunkSizeBytes int) string {
	storageBinaryPath, err := getBinaryPath("BB_STORAGE_RUNFILE_PATH")
	require.NoError(t, err, "Could not get bb_storage binary path")

	socketPath := createSocketPath(t)
	setupServer(t, storageBinaryPath, "standalone", standaloneCASConfig(socketPath, minChunkSizeBytes))

	require.Eventually(t, func() bool {
		_, err := os.Stat(socketPath)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "Standalone storage server did not start.")

	return socketPath
}

func bbCopyConfig(sourceSocket, sinkSocket string, actionDigests, blobDigests, dirDigests, treeDigests []digest.Digest) string {
	type jsonDigest struct {
		Hash      string `json:"hash"`
		SizeBytes int64  `json:"sizeBytes"`
	}

	jsonify := func(digests []digest.Digest) string {
		if len(digests) == 0 {
			return "[]"
		}

		jds := make([]jsonDigest, 0, len(digests))
		for _, d := range digests {
			jds = append(jds, jsonDigest{
				Hash:      d.GetHashString(),
				SizeBytes: d.GetSizeBytes(),
			})
		}

		data, err := json.Marshal(jds)
		if err != nil {
			panic(err)
		}
		return string(data)
	}

	return fmt.Sprintf(`
local sourcePath = '%s';
local sinkPath = '%s';
local actions = %s;
local blobs = %s;
local directories = %s;
local trees = %s;
local maximumMessageSizeBytes = %d;

{
	source: {
		chunkStorage: {
			grpc: {
				client: { address: "unix:" + sourcePath },
			},
		},
		chunkListStorage:  {
			grpc: {
				client: { address: "unix:" + sourcePath },
			},
		},
		cdcParameterCache: {
			cacheSize: 1,
			cacheDuration: '60s',
			cacheReplacementPolicy: 'LEAST_RECENTLY_USED',
		},
	},
	sink: {
		chunkStorage: {
			grpc: {
				client: { address: "unix:" + sinkPath },
			},
		},
		chunkListStorage:  {
			grpc: {
				client: { address: "unix:" + sinkPath },
			},
		},
		cdcParameterCache: {
			cacheSize: 1,
			cacheDuration: '60s',
			cacheReplacementPolicy: 'LEAST_RECENTLY_USED',
		},
	},
	maximumMessageSizeBytes: maximumMessageSizeBytes,
	instanceName: 'allowed_instance',
	digestFunction: 'SHA256',
	traversalConcurrency: 1,
	actions: actions,
	blobs: blobs,
	directories: directories,
	trees: trees,
}
`, escapeJSON(sourceSocket), escapeJSON(sinkSocket), jsonify(actionDigests), jsonify(blobDigests), jsonify(dirDigests), jsonify(treeDigests), maximumMessageSizeBytes)
}

func runBBCopy(t *testing.T, sourceSocket, sinkSocket string, actionDigests, blobDigests, dirDigests, treeDigests []digest.Digest) error {
	bbCopyBinaryPath, err := getBinaryPath("BB_COPY_RUNFILE_PATH")
	require.NoError(t, err, "Could not get bb_copy binary path")

	file, err := writeConfigFile("bb_copy", bbCopyConfig(sourceSocket, sinkSocket, actionDigests, blobDigests, dirDigests, treeDigests))
	require.NoError(t, err, "Could not write bb_copy config file")

	t.Cleanup(func() {
		os.Remove(file.Name())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bbCopyBinaryPath, file.Name())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func TestBBCopyBlobReplication(t *testing.T) {
	ctx := t.Context()
	smallerMinChunkSizeBytes := 64 << 10
	largerMinChunkSizeBytes := 256 << 10
	t.Run("Noop", func(t *testing.T) {
		sourceSocket := setupCluster(t, smallerMinChunkSizeBytes)
		sinkSocket := setupStandaloneCAS(t, largerMinChunkSizeBytes)
		require.NoError(t, runBBCopy(t, sourceSocket, sinkSocket, nil, nil, nil, nil))
	})

	t.Run("CopyFromLargerChunkSource", func(t *testing.T) {
		sourceSocket := setupCluster(t, smallerMinChunkSizeBytes)
		_, sourceCASClient, _, _ := createClients(t, sourceSocket)
		sinkSocket := setupStandaloneCAS(t, largerMinChunkSizeBytes)
		_, sinkCASClient, _, _ := createClients(t, sinkSocket)

		data := makeRandomData(t, largerMinChunkSizeBytes*2, 0)
		dataDigest := computeDigest(data)

		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, data, dataDigest))
		require.NoError(t, runBBCopy(t, sourceSocket, sinkSocket, nil, []digest.Digest{dataDigest}, nil, nil))
		missing, err := findMissingBlobs(ctx, sinkCASClient, []digest.Digest{dataDigest})
		require.NoError(t, err)
		require.Empty(t, missing)
	})

	t.Run("CopyFromSmallerChunkSource", func(t *testing.T) {
		sourceSocket := setupCluster(t, largerMinChunkSizeBytes)
		_, sourceCASClient, _, _ := createClients(t, sourceSocket)
		sinkSocket := setupStandaloneCAS(t, smallerMinChunkSizeBytes)
		_, sinkCASClient, _, _ := createClients(t, sinkSocket)

		data := makeRandomData(t, largerMinChunkSizeBytes*2, 0)
		dataDigest := computeDigest(data)

		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, data, dataDigest))
		require.NoError(t, runBBCopy(t, sourceSocket, sinkSocket, nil, []digest.Digest{dataDigest}, nil, nil))
		missing, err := findMissingBlobs(ctx, sinkCASClient, []digest.Digest{dataDigest})
		require.NoError(t, err)
		require.Empty(t, missing)
	})

	t.Run("CopyMissingChunk", func(t *testing.T) {
		sourceSocket := setupCluster(t, largerMinChunkSizeBytes)
		sinkSocket := setupStandaloneCAS(t, smallerMinChunkSizeBytes)
		_, sinkCASClient, _, _ := createClients(t, sinkSocket)

		data := makeRandomData(t, largerMinChunkSizeBytes*2, 0)
		dataDigest := computeDigest(data)

		require.Error(t, runBBCopy(t, sourceSocket, sinkSocket, nil, []digest.Digest{dataDigest}, nil, nil))
		missing, err := findMissingBlobs(ctx, sinkCASClient, []digest.Digest{dataDigest})
		require.NoError(t, err)
		require.NotEmpty(t, missing)
	})
}

func TestBBCopyCompositeReplication(t *testing.T) {
	ctx := t.Context()
	// Set the min chunk size bytes to the minimum value of 64 to force
	// the proto objects into multiple chunks.
	minChunkSizeBytes := 64

	t.Run("CopyDirectory", func(t *testing.T) {
		sourceSocket := setupCluster(t, minChunkSizeBytes)
		_, sourceCASClient, _, _ := createClients(t, sourceSocket)
		sinkSocket := setupStandaloneCAS(t, minChunkSizeBytes)
		_, sinkCASClient, _, _ := createClients(t, sinkSocket)

		fileData := makeRandomData(t, 1024, 0)
		fileDigest := computeDigest(fileData)

		subDir := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{Name: "some very long file name that will cause the directory object to be split into multiple chunks", Digest: fileDigest.GetProto()},
			},
		}
		subDirData, err := proto.Marshal(subDir)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(subDirData), 2*minChunkSizeBytes)
		subDirDigest := computeDigest(subDirData)

		rootDir := &remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{Name: "some very long directory name that will cause the root directory object to be split into multiple chunks", Digest: subDirDigest.GetProto()},
			},
		}
		rootDirData, err := proto.Marshal(rootDir)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(rootDirData), 2*minChunkSizeBytes)
		rootDirDigest := computeDigest(rootDirData)

		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, fileData, fileDigest))
		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, subDirData, subDirDigest))
		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, rootDirData, rootDirDigest))

		require.NoError(t, runBBCopy(t, sourceSocket, sinkSocket, nil, nil, []digest.Digest{rootDirDigest}, nil))

		missing, err := findMissingBlobs(ctx, sinkCASClient, []digest.Digest{fileDigest, subDirDigest, rootDirDigest})
		require.NoError(t, err)
		require.Empty(t, missing)
	})

	t.Run("CopyTreeWithCDC", func(t *testing.T) {
		sourceSocket := setupCluster(t, minChunkSizeBytes)
		_, sourceCASClient, _, _ := createClients(t, sourceSocket)
		sinkSocket := setupStandaloneCAS(t, minChunkSizeBytes)
		_, sinkCASClient, _, _ := createClients(t, sinkSocket)

		fileData := makeRandomData(t, 1024, 0)
		fileDigest := computeDigest(fileData)

		tree := &remoteexecution.Tree{
			Root: &remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{Name: "some long file name that will cause the tree object to be split into multiple chunks ", Digest: fileDigest.GetProto()},
				},
			},
		}
		treeData, err := proto.Marshal(tree)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(treeData), 2*minChunkSizeBytes)
		treeDigest := computeDigest(treeData)

		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, fileData, fileDigest))
		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, treeData, treeDigest))

		require.NoError(t, runBBCopy(t, sourceSocket, sinkSocket, nil, nil, nil, []digest.Digest{treeDigest}))

		missing, err := findMissingBlobs(ctx, sinkCASClient, []digest.Digest{fileDigest, treeDigest})
		require.NoError(t, err)
		require.Empty(t, missing)
	})

	t.Run("CopyActionWithCDC", func(t *testing.T) {
		sourceSocket := setupCluster(t, minChunkSizeBytes)
		_, sourceCASClient, _, _ := createClients(t, sourceSocket)
		sinkSocket := setupStandaloneCAS(t, minChunkSizeBytes)
		_, sinkCASClient, _, _ := createClients(t, sinkSocket)

		fileData := makeRandomData(t, 1024, 0)
		fileDigest := computeDigest(fileData)

		dir := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{Name: "input.txt", Digest: fileDigest.GetProto()},
			},
		}
		dirData, err := proto.Marshal(dir)
		require.NoError(t, err)
		dirDigest := computeDigest(dirData)

		longArgument := strings.Repeat("a", 1024)
		cmd := &remoteexecution.Command{
			Arguments: []string{"tool", longArgument},
		}
		cmdData, err := proto.Marshal(cmd)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(cmdData), 2*minChunkSizeBytes)
		cmdDigest := computeDigest(cmdData)

		action := &remoteexecution.Action{
			CommandDigest:   cmdDigest.GetProto(),
			InputRootDigest: dirDigest.GetProto(),
		}
		actionData, err := proto.Marshal(action)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(actionData), 2*minChunkSizeBytes)
		actionDigest := computeDigest(actionData)

		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, fileData, fileDigest))
		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, dirData, dirDigest))
		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, cmdData, cmdDigest))
		require.NoError(t, batchUploadBlob(ctx, sourceCASClient, actionData, actionDigest))

		require.NoError(t, runBBCopy(t, sourceSocket, sinkSocket, []digest.Digest{actionDigest}, nil, nil, nil))

		missing, err := findMissingBlobs(ctx, sinkCASClient, []digest.Digest{fileDigest, dirDigest, cmdDigest, actionDigest})
		require.NoError(t, err)
		require.Empty(t, missing)
	})
}
