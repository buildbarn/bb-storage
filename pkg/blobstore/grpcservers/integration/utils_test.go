package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type storageAddresses struct {
	storageShardSocketPaths []string
	localCacheSocketPath    string
}

const (
	maximumMessageSizeBytes       = 2 << 20 // 2MiB
	maximumChunkCount             = 1000
	bytestreamWriteChunkSizeBytes = 1 << 20 // 1MiB
)

func storageConfig(listenSocketPath string, minChunkSizeBytes int) string {
	return fmt.Sprintf(`
local listenPath = '%s';
local maximumMessageSizeBytes = %d;
local maximumChunkCount = %d;
local minChunkSizeBytes = %d;

local inMemoryStorage = {
	keyLocationMap: {
		inMemory: { entries: 1024 },
		maximumGetAttempts: 32,
		maximumPutAttempts: 64,
	},
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
				'local': inMemoryStorage + { chunkingParameters: { minChunkSizeBytes: minChunkSizeBytes, horizonSizeBytes: 8*minChunkSizeBytes } },
			},
			chunkMappingStorage: {
				'local': inMemoryStorage,
			},
		},
		maximumChunkCount: maximumChunkCount,
		getAuthorizer: { allow: {} },
		putAuthorizer: { allow: {} },
		findMissingAuthorizer: { allow: {} },
	},
	actionCache: {
		backend: { 'local': inMemoryStorage },
		getAuthorizer: { allow: {} },
		putAuthorizer: { allow: {} },
	},
}
`, escapeJSON(listenSocketPath), maximumMessageSizeBytes, maximumChunkCount, minChunkSizeBytes)
}

func replicatorConfig(listenSocketPath string, params storageAddresses) string {
	upstreamsJSON, err := json.Marshal(params.storageShardSocketPaths)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`
local listenPath = '%s';
local upstreamAddresses = %s;
local localCacheAddress = "unix:%s";
local maximumMessageSizeBytes = %d;

local topology = {
	sharding: {
		shards: {
			["shard" + i]: { backend: { grpc: { client: { address: "unix:" + upstreamAddresses[i] } } }, weight: 1 }
			for i in std.range(0, std.length(upstreamAddresses) - 1)
		}
	}
};

{
	grpcServers: [{
		listenPaths: [listenPath],
		authenticationPolicy: { allow: {} },
	}],
	maximumMessageSizeBytes: maximumMessageSizeBytes,
	source: topology,
	sink: { grpc: { client: { address: localCacheAddress } } },
	replicator: {
		deduplicating: {
			concurrencyLimiting: {
				base: { 'local': {} },
				maximumConcurrency: 1,
			},
		},
	}
}
`, escapeJSON(listenSocketPath), upstreamsJSON, escapeJSON(params.localCacheSocketPath), maximumMessageSizeBytes)
}

func frontendConfig(listenSocketPath string, params storageAddresses) string {
	upstreamsJSON, err := json.Marshal(params.storageShardSocketPaths)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`
local listenPath = '%s';
local upstreamAddresses = %s;
local localCacheAddress = "unix:%s";
local maximumMessageSizeBytes = %d;
local maximumChunkCount = %d;

local topology = {
	sharding: {
		shards: {
			["shard" + i]: {
				backend: {
					grpc: {
						client: { address: "unix:" + upstreamAddresses[i] },
					},
				},
				weight: 1,
			}
			for i in std.range(0, std.length(upstreamAddresses) - 1)
		}
	}
};

local readCaching(inner, replicator) = {
	readCaching: {
		fast: { grpc: { client: { address: localCacheAddress } } },
		slow: inner,
		replicator: replicator,
	},
};

local authorizer = {
	jmespathExpression: {
		expression: 'contains(authenticationMetadata.private.mayAccess, instanceName)'
	},
};

local csReplicator = { 'deduplicating': { 'local': {} } };
local simpleReplicator = { 'local': {} };

{
	grpcServers: [{
		listenPaths: [listenPath],
		authenticationPolicy: {
			allow: {
				private: {
					mayAccess: ['allowed_instance'],
				},
			},
		},
	}],
	supportedCompressors: ['ZSTD'],
	zstdPool: {
		maximumEncoders: 16,
		maximumDecoders: 16,
		encoderWindowSizeBytes: 8 * 1024 * 1024,
		decoderWindowSizeBytes: 8 * 1024 * 1024,
	},
	maximumMessageSizeBytes: maximumMessageSizeBytes,
	contentAddressableStorageServer: {
		contentAddressableStorage: {
			chunkStorage: readCaching(topology, csReplicator),
			chunkMappingStorage: readCaching({ chunkMappingValidating: { backend: topology } }, simpleReplicator),
			cdcParameterCache: {
				cacheSize: 1,
				cacheDuration: '60s',
				cacheReplacementPolicy: 'LEAST_RECENTLY_USED',
			},
		},
		maximumChunkCount: maximumChunkCount,
		getAuthorizer: authorizer,
		putAuthorizer: authorizer,
		findMissingAuthorizer: authorizer,
	},
	actionCache: {
		backend: readCaching(
			{
				completenessChecking: {
					backend: topology,
					maximumTotalTreeSizeBytes: 64 * 1024 * 1024,
				},
			},
			simpleReplicator,
		),
		getAuthorizer: authorizer,
		putAuthorizer: authorizer,
	},
}
`, escapeJSON(listenSocketPath), upstreamsJSON, escapeJSON(params.localCacheSocketPath), maximumMessageSizeBytes, maximumChunkCount)
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

func getBinaryPath(envVar string) (string, error) {
	rf, err := runfiles.New()
	if err != nil {
		return "", util.StatusWrap(err, "Failed to initialize runfiles")
	}

	runfilePath := os.Getenv(envVar)
	if runfilePath == "" {
		return "", util.StatusWrapf(err, "'%s' environment variable is not set", envVar)
	}

	return rf.Rlocation(runfilePath)
}

func setupCluster(t *testing.T, minChunkSizeBytes int) string {
	storageBinaryPath, err := getBinaryPath("BB_STORAGE_RUNFILE_PATH")
	require.NoError(t, err, "Could not get bb_storage binary path")

	storageSocketPaths := make([]string, 2)
	for i := 0; i < 2; i++ {
		storageSocketPaths[i] = createSocketPath(t)
		setupServer(t, storageBinaryPath, fmt.Sprintf("storage%d", i), storageConfig(storageSocketPaths[i], minChunkSizeBytes))
		require.Eventually(t, func() bool {
			_, err := os.Stat(storageSocketPaths[i])
			return err == nil
		}, 2*time.Second, 10*time.Millisecond, "Storage server %d did not start.", i)
	}

	localCacheSocketPath := createSocketPath(t)
	setupServer(t, storageBinaryPath, "localCache", storageConfig(localCacheSocketPath, minChunkSizeBytes))
	require.Eventually(t, func() bool {
		_, err := os.Stat(localCacheSocketPath)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "Local cache storage server did not start.")

	storageAddresses := storageAddresses{
		storageShardSocketPaths: storageSocketPaths,
		localCacheSocketPath:    localCacheSocketPath,
	}

	frontendSocketPath := createSocketPath(t)
	setupServer(t, storageBinaryPath, "frontend", frontendConfig(frontendSocketPath, storageAddresses))
	require.Eventually(t, func() bool {
		_, err := os.Stat(frontendSocketPath)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "Frontend server did not start.")

	return frontendSocketPath
}

func createClients(t *testing.T, socketPath string) (remoteexecution.CapabilitiesClient, remoteexecution.ContentAddressableStorageClient, remoteexecution.ActionCacheClient, bytestream.ByteStreamClient) {
	conn, err := grpc.NewClient(fmt.Sprintf("unix:%s", socketPath), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
	})

	return remoteexecution.NewCapabilitiesClient(conn),
		remoteexecution.NewContentAddressableStorageClient(conn),
		remoteexecution.NewActionCacheClient(conn),
		bytestream.NewByteStreamClient(conn)
}

func setupServer(t *testing.T, binaryPath, name, config string) {
	configFile, err := writeConfigFile(name, config)
	require.NoError(t, err, "Could not write config file")

	cmd := exec.Command(binaryPath, configFile.Name())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.Remove(configFile.Name())
	})
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

func computeDigestWithInstanceName(data []byte, instanceName string) digest.Digest {
	hash := sha256.Sum256(data)
	return digest.MustNewDigest(
		instanceName,
		remoteexecution.DigestFunction_SHA256,
		hex.EncodeToString(hash[:]),
		int64(len(data)),
	)
}

func computeDigest(data []byte) digest.Digest {
	return computeDigestWithInstanceName(data, "allowed_instance")
}

func escapeJSON(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func makeRandomData(t *testing.T, size int, seed int64) []byte {
	t.Helper()
	data := make([]byte, size)
	r := rand.New(rand.NewSource(seed))
	_, err := r.Read(data)
	require.NoError(t, err)
	return data
}

func zstdEncode(data []byte) []byte {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil)
}

func zstdDecode(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
	defer decoder.Close()
	ret, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func batchUploadBlob(ctx context.Context, client remoteexecution.ContentAddressableStorageClient, data []byte, d digest.Digest, compressor remoteexecution.Compressor_Value) error {
	payload := data
	if compressor == remoteexecution.Compressor_ZSTD {
		payload = zstdEncode(data)
	}
	req := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName: d.GetInstanceName().String(),
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest:     d.GetProto(),
				Data:       payload,
				Compressor: compressor,
			},
		},
	}

	responses, err := client.BatchUpdateBlobs(ctx, req)
	if err != nil {
		return err
	}
	for _, response := range responses.Responses {
		return status.ErrorProto(response.Status)
	}
	return err
}

func batchDownloadBlobs(ctx context.Context, client remoteexecution.ContentAddressableStorageClient, digests []digest.Digest, compressor remoteexecution.Compressor_Value) ([][]byte, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	dataMap := make(map[string][]byte, len(digests))

	batchStart := 0
	for batchStart < len(digests) {
		batchSize := int64(0)
		batchEnd := batchStart

		// Slide the window forward until we hit our byte limit. We
		// always include at least one digest, even if it exceeds the
		// limit.
		for batchEnd < len(digests) {
			size := digests[batchEnd].GetSizeBytes()
			if batchEnd > batchStart && batchSize+size > maximumMessageSizeBytes/2 {
				break
			}
			batchSize += size
			batchEnd++
		}

		// Prepare the batch request.
		var batchProtos []*remoteexecution.Digest
		for _, d := range digests[batchStart:batchEnd] {
			batchProtos = append(batchProtos, d.GetProto())
		}

		req := &remoteexecution.BatchReadBlobsRequest{
			InstanceName:          digests[0].GetInstanceName().String(),
			Digests:               batchProtos,
			AcceptableCompressors: []remoteexecution.Compressor_Value{compressor},
		}

		// Execute the RPC for this specific batch.
		res, err := client.BatchReadBlobs(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, r := range res.Responses {
			if r.Status != nil && r.Status.Code != int32(codes.OK) {
				return nil, status.ErrorProto(r.Status)
			}
			dataMap[r.Digest.Hash] = r.Data
		}

		// Move the window forward for the next batch.
		batchStart = batchEnd
	}

	// Map the responses back to the requested order.
	var downloadedData [][]byte
	for _, d := range digests {
		data, ok := dataMap[d.GetHashString()]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "Digest %s was not returned in BatchReadBlobs response", d.GetHashString())
		}
		downloadedData = append(downloadedData, data)
	}

	return downloadedData, nil
}

func findMissingBlobs(ctx context.Context, client remoteexecution.ContentAddressableStorageClient, digests []digest.Digest) ([]digest.Digest, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	var digestProtos []*remoteexecution.Digest
	for _, d := range digests {
		digestProtos = append(digestProtos, d.GetProto())
	}

	req := &remoteexecution.FindMissingBlobsRequest{
		InstanceName: digests[0].GetInstanceName().String(),
		BlobDigests:  digestProtos,
	}

	res, err := client.FindMissingBlobs(ctx, req)
	if err != nil {
		return nil, err
	}

	var missingDigests []digest.Digest
	digestFunction := digests[0].GetDigestFunction()
	for _, p := range res.MissingBlobDigests {
		d, err := digestFunction.NewDigestFromProto(p)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse missing digest from proto")
		}
		missingDigests = append(missingDigests, d)
	}

	return missingDigests, nil
}

func splitBlob(ctx context.Context, client remoteexecution.ContentAddressableStorageClient, d digest.Digest) ([]digest.Digest, error) {
	req := &remoteexecution.SplitBlobRequest{
		InstanceName:     d.GetInstanceName().String(),
		BlobDigest:       d.GetProto(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}

	res, err := client.SplitBlob(ctx, req)
	if err != nil {
		return nil, err
	}

	chunkDigests := make([]digest.Digest, 0, len(res.ChunkDigests))
	digestFunction := d.GetDigestFunction()
	for _, chunkProto := range res.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkProto)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse chunk digest from proto")
		}
		chunkDigests = append(chunkDigests, chunkDigest)
	}

	return chunkDigests, nil
}

func getChunkMapping(ctx context.Context, client remoteexecution.ContentAddressableStorageClient, d digest.Digest) ([]digest.Digest, error) {
	stream, err := client.GetChunkMapping(ctx, &remoteexecution.GetChunkMappingRequest{
		InstanceName:     d.GetInstanceName().String(),
		BlobDigest:       d.GetProto(),
		DigestFunction:   d.GetDigestFunction().GetEnumValue(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	})
	if err != nil {
		return nil, err
	}

	digestFunction := d.GetDigestFunction()
	var chunkDigests []digest.Digest
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			return chunkDigests, nil
		} else if err != nil {
			return nil, err
		}
		for _, chunkProto := range res.ChunkDigests {
			chunkDigest, err := digestFunction.NewDigestFromProto(chunkProto)
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to parse chunk digest from proto")
			}
			chunkDigests = append(chunkDigests, chunkDigest)
		}
	}
}

func spliceBlob(ctx context.Context, client remoteexecution.ContentAddressableStorageClient, blob digest.Digest, chunks []digest.Digest) error {
	chunkDigests := make([]*remoteexecution.Digest, 0, len(chunks))
	for _, d := range chunks {
		chunkDigests = append(chunkDigests, d.GetProto())
	}

	req := &remoteexecution.SpliceBlobRequest{
		InstanceName:     blob.GetInstanceName().String(),
		BlobDigest:       blob.GetProto(),
		ChunkDigests:     chunkDigests,
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}

	_, err := client.SpliceBlob(ctx, req)
	return err
}

func registerChunkMapping(ctx context.Context, client remoteexecution.ContentAddressableStorageClient, blob digest.Digest, chunks []digest.Digest) error {
	stream, err := client.RegisterChunkMapping(ctx)
	if err != nil {
		return err
	}
	chunkDigests := make([]*remoteexecution.Digest, 0, len(chunks))
	for _, d := range chunks {
		chunkDigests = append(chunkDigests, d.GetProto())
	}
	if err := stream.Send(&remoteexecution.RegisterChunkMappingRequest{
		InstanceName:     blob.GetInstanceName().String(),
		BlobDigest:       blob.GetProto(),
		ChunkDigests:     chunkDigests,
		DigestFunction:   blob.GetDigestFunction().GetEnumValue(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}); err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	return err
}

func bytestreamWriteBlob(ctx context.Context, client bytestream.ByteStreamClient, data []byte, digest digest.Digest, compressor remoteexecution.Compressor_Value) error {
	writeStream, err := client.Write(ctx)
	if err != nil {
		return err
	}
	payload := data
	if compressor == remoteexecution.Compressor_ZSTD {
		payload = zstdEncode(data)
	}
	offset := int64(0)
	dataSize := int64(len(payload))

	if dataSize == 0 {
		// An empty upload still consists of a single request that
		// finishes the write, otherwise the server cannot even
		// identify the resource being written.
		if err := writeStream.Send(&bytestream.WriteRequest{
			ResourceName: digest.GetByteStreamWritePath(uuid.New(), compressor),
			FinishWrite:  true,
		}); err != nil {
			return util.StatusFromMultiple([]error{err, io.EOF})
		}
		_, err = writeStream.CloseAndRecv()
		return err
	}

	for offset < dataSize {
		end := offset + bytestreamWriteChunkSizeBytes
		if end > dataSize {
			end = dataSize
		}

		chunk := payload[offset:end]
		isLast := end == dataSize

		err := writeStream.Send(&bytestream.WriteRequest{
			ResourceName: digest.GetByteStreamWritePath(uuid.New(), compressor),
			WriteOffset:  offset,
			FinishWrite:  isLast,
			Data:         chunk,
		})
		if err == io.EOF {
			_, innerErr := writeStream.CloseAndRecv()
			if innerErr != nil {
				return util.StatusFromMultiple([]error{innerErr, err})
			}
		} else if err != nil {
			return err
		}
		offset = end
	}
	_, err = writeStream.CloseAndRecv()
	return err
}

func bytestreamReadBlob(ctx context.Context, client bytestream.ByteStreamClient, digest digest.Digest, compressor remoteexecution.Compressor_Value) ([]byte, error) {
	readReq := &bytestream.ReadRequest{
		ResourceName: digest.GetByteStreamReadPath(compressor),
	}
	readStream, err := client.Read(ctx, readReq)
	if err != nil {
		return nil, err
	}
	defer readStream.CloseSend()

	receivedData := make([]byte, 0, digest.GetSizeBytes())
	for {
		res, err := readStream.Recv()
		if err == io.EOF {
			return receivedData, nil
		} else if err != nil {
			return nil, err
		}
		receivedData = append(receivedData, res.Data...)
	}
}

func updateActionResult(ctx context.Context, acClient remoteexecution.ActionCacheClient, actionDigest digest.Digest, result *remoteexecution.ActionResult) error {
	_, err := acClient.UpdateActionResult(ctx, &remoteexecution.UpdateActionResultRequest{
		InstanceName: actionDigest.GetInstanceName().String(),
		ActionDigest: actionDigest.GetProto(),
		ActionResult: result,
	})
	return err
}

func getActionResult(ctx context.Context, acClient remoteexecution.ActionCacheClient, actionDigest digest.Digest) (*remoteexecution.ActionResult, error) {
	return acClient.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
		InstanceName: actionDigest.GetInstanceName().String(),
		ActionDigest: actionDigest.GetProto(),
	})
}
