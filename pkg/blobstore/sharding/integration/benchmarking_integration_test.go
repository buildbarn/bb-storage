package integration

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/rules_go/go/runfiles"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const storageConfig = `
{
	grpcServers: [{
		listenAddresses: [':8981'],
		authenticationPolicy: { allow: {} },
	}],
	maximumMessageSizeBytes: 4 * 1024 * 1024,
	contentAddressableStorage: {
		backend: {
			'local': {
				keyLocationMapInMemory: { entries: 16 },
				keyLocationMapMaximumGetAttempts: 32,
				keyLocationMapMaximumPutAttempts: 64,
				oldBlocks: 8,
				currentBlocks: 24,
				newBlocks: 3,
				blocksInMemory: { blockSizeBytes: 32 },
			},
		},
		getAuthorizer: { allow: {} },
		putAuthorizer: { allow: {} },
		findMissingAuthorizer: { allow: {} },
	},
}
`

const frontendConfig = `
local shardCount = std.parseInt(std.extVar('SHARD_COUNT'));
{
	grpcServers: [{
		listenAddresses: [':8980'],
		authenticationPolicy: { allow: {} },
	}],
	maximumMessageSizeBytes: 4 * 1024 * 1024,
	contentAddressableStorage: {
		backend: {
			sharding: {
				shards: {
					[std.toString(i)]: {
						weight: 1,
						backend: { grpc: { address: 'localhost:8981' } },
					}
					for i in std.range(0, shardCount - 1)
				}
			},
		},
		getAuthorizer: { allow: {} },
		putAuthorizer: { allow: {} },
		findMissingAuthorizer: { allow: {} },
	},
}
`

const legacyConfig = `
local shardCount = std.parseInt(std.extVar('SHARD_COUNT'));
{
	grpcServers: [{
		listenAddresses: [':8980'],
		authenticationPolicy: { allow: {} },
	}],
	maximumMessageSizeBytes: 4 * 1024 * 1024,
	contentAddressableStorage: {
		backend: {
			sharding: {
				shards: {
					[std.toString(i)]: {
						weight: 1,
						backend: { grpc: { address: 'localhost:8981' } },
					}
					for i in std.range(0, shardCount - 1)
				},
				legacy: {
					shardOrder: [
						std.toString(i)
						for i in std.range(0, shardCount - 1)
					]
				}
			},
		},
		getAuthorizer: { allow: {} },
		putAuthorizer: { allow: {} },
		findMissingAuthorizer: { allow: {} },
	},
}
`

// waitForTCP repeatedly tries to establish a TCP connection to addr until timeout.
func waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for TCP %s", addr)
}

func calcMd5(n uint32) string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(n))
	hash := md5.Sum(b)
	return fmt.Sprintf("%x", hash)
}

func callFMB(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send a command to server F.
	_, err = conn.Write([]byte("ping"))
	if err != nil {
		return err
	}

	// Read the response (for example purposes, we ignore its content).
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	return err
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

type component struct {
	name   string
	config string
}

func performBenchmark(b *testing.B, components []component, shardCount int) {
	const digestCount = 1000
	rf, err := runfiles.New()
	if err != nil {
		b.Fatalf("failed to intialize runfiles: %v", err)
	}
	bbStoragePath, err := rf.Rlocation("_main/cmd/bb_storage/bb_storage_/bb_storage")
	if err != nil {
		b.Fatalf("failed to find runfiles: %v", err)
	}
	for _, component := range components {
		file, err := writeConfigFile(component.name, component.config)
		if err != nil {
			b.Fatalf("failed to write config file for %q: %v", component.name, err)
		}
		defer os.Remove(file.Name())
		cmd := exec.Command(bbStoragePath, file.Name())
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = append(os.Environ(), fmt.Sprintf("SHARD_COUNT=%d", shardCount))
		if err := cmd.Start(); err != nil {
			b.Fatalf("failed to start component %q: %v", component.name, err)
		}
		defer func() {
			cmd.Process.Kill()
			cmd.Wait()
		}()
	}

	if err := waitForTCP("127.0.0.1:8980", 5*time.Second); err != nil {
		b.Fatalf("frontend did not start in time: %v", err)
	}

	digests := make([]*remoteexecution.Digest, digestCount)
	for i := 0; i < digestCount; i++ {
		digests[i] = &remoteexecution.Digest{
			Hash:      calcMd5(uint32(i)),
			SizeBytes: 4,
		}
	}
	req := &remoteexecution.FindMissingBlobsRequest{
		BlobDigests: digests,
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		conn, err := grpc.NewClient("127.0.0.1:8980", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			b.Fatalf("failed to connect to frontend: %v", err)
		}
		defer conn.Close()
		client := remoteexecution.NewContentAddressableStorageClient(conn)
		for pb.Next() {
			_, err = client.FindMissingBlobs(ctx, req)
			if err != nil {
				b.Fatalf("failed to call FindMissingBlobs: %v", err)
			}
		}
	})
	b.StopTimer()
}

func BenchmarkSharding10(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "frontend", config: frontendConfig},
	}
	performBenchmark(b, components, 10)
}

func BenchmarkSharding100(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "frontend", config: frontendConfig},
	}
	performBenchmark(b, components, 100)
}

func BenchmarkSharding1000(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "frontend", config: frontendConfig},
	}
	performBenchmark(b, components, 1000)
}

func BenchmarkSharding10000(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "frontend", config: frontendConfig},
	}
	performBenchmark(b, components, 10000)
}

func BenchmarkLegacy10(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "legacy", config: legacyConfig},
	}
	performBenchmark(b, components, 10)
}

func BenchmarkLegacy100(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "legacy", config: legacyConfig},
	}
	performBenchmark(b, components, 100)
}

func BenchmarkLegacy1000(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "legacy", config: legacyConfig},
	}
	performBenchmark(b, components, 1000)
}

func BenchmarkLegacy10000(b *testing.B) {
	components := []component{
		{name: "storage", config: storageConfig},
		{name: "legacy", config: legacyConfig},
	}
	performBenchmark(b, components, 10000)
}
