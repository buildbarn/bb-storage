package grpcclients

import (
	"context"
	"io"
	"slices"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const _defaultCompressionThresholdBytes = 1024 // 1KB

type casWithZstdBlobAccess struct {
	uncompressedBlobAccess          blobstore.BlobAccess
	byteStreamClient                bytestream.ByteStreamClient
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	capabilitiesClient              remoteexecution.CapabilitiesClient
	uuidGenerator                   util.UUIDGenerator
	readChunkSize                   int
	compressionThresholdBytes       int64
}

// NewCASWithZstdBlobAccess creates a BlobAccess handle that uses ZSTD compression
// for blobs larger than the threshold, and falls back to non-compressed access
// for smaller blobs.
func NewCASWithZstdBlobAccess(
	client grpc.ClientConnInterface,
	uuidGenerator util.UUIDGenerator,
	readChunkSize int,
	compressionThresholdBytes int64,
) blobstore.BlobAccess {
	if compressionThresholdBytes == 0 {
		compressionThresholdBytes = _defaultCompressionThresholdBytes
	}

	return &casWithZstdBlobAccess{
		uncompressedBlobAccess:          NewCASBlobAccess(client, uuidGenerator, readChunkSize),
		byteStreamClient:                bytestream.NewByteStreamClient(client),
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		capabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		uuidGenerator:                   uuidGenerator,
		readChunkSize:                   readChunkSize,
		compressionThresholdBytes:       compressionThresholdBytes,
	}
}

type zstdByteStreamChunkReader struct {
	client        bytestream.ByteStream_ReadClient
	cancel        context.CancelFunc
	zstdReader    io.ReadCloser
	readChunkSize int
	wg            sync.WaitGroup
}

// Read opens a pipe which allows us to process the compressed stream into the ZSTD
// reader without blocking or keeping chunks in memory.
//
// Unlike the non-compressed version that can return gRPC chunks directly, compression
// requires bridging two incompatible interfaces:
// - gRPC pushes chunks to us via client.Recv().
// - ZSTD expects to pull data from us via Read().
//
// We work around this by using a goroutine that receives gRPC chunks and writes them to a pipe,
// while the ZSTD decoder reads from the other end of the pipe. This creates a streaming
// pipeline: gRPC -> goroutine -> pipe -> ZSTD -> CAS.
func (r *zstdByteStreamChunkReader) Read() ([]byte, error) {
	if r.zstdReader == nil {
		pr, pw := io.Pipe()

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer pw.Close()
			for {
				chunk, err := r.client.Recv()
				if err != nil {
					if err != io.EOF {
						pw.CloseWithError(err)
					}
					return
				}
				if _, writeErr := pw.Write(chunk.Data); writeErr != nil {
					pw.CloseWithError(writeErr)
					return
				}
			}
		}()

		var err error
		r.zstdReader, err = util.NewZstdReadCloser(pr, zstd.WithDecoderConcurrency(1))
		if err != nil {
			pr.Close()
			return nil, err
		}
	}

	buf := make([]byte, r.readChunkSize)
	n, err := r.zstdReader.Read(buf)
	if n > 0 {
		if err != nil && err != io.EOF {
			err = nil
		}
		return buf[:n], err
	}
	return nil, err
}

func (r *zstdByteStreamChunkReader) Close() {
	if r.zstdReader != nil {
		r.zstdReader.Close()
	}
	r.cancel()

	// Drain the gRPC stream.
	for {
		if _, err := r.client.Recv(); err != nil {
			break
		}
	}
	r.wg.Wait()
}

func (ba *casWithZstdBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	if digest.GetSizeBytes() < ba.compressionThresholdBytes {
		return ba.uncompressedBlobAccess.Get(ctx, digest)
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	resourceName := digest.GetByteStreamReadPath(remoteexecution.Compressor_ZSTD)
	client, err := ba.byteStreamClient.Read(
		metadata.AppendToOutgoingContext(ctxWithCancel, resourceNameHeader, resourceName),
		&bytestream.ReadRequest{
			ResourceName: resourceName,
		},
	)
	if err != nil {
		cancel()
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewCASBufferFromChunkReader(digest, &zstdByteStreamChunkReader{
		client:        client,
		cancel:        cancel,
		readChunkSize: ba.readChunkSize,
	}, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *casWithZstdBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "GetFromComposite is not supported with ZSTD compression"))
}

type zstdByteStreamWriter struct {
	client       bytestream.ByteStream_WriteClient
	resourceName string
	writeOffset  int64
	cancel       context.CancelFunc
}

func (w *zstdByteStreamWriter) Write(p []byte) (int, error) {
	if err := w.client.Send(&bytestream.WriteRequest{
		ResourceName: w.resourceName,
		WriteOffset:  w.writeOffset,
		Data:         p,
	}); err != nil {
		return 0, err
	}
	w.writeOffset += int64(len(p))
	w.resourceName = ""
	return len(p), nil
}

func (w *zstdByteStreamWriter) Close() error {
	if err := w.client.Send(&bytestream.WriteRequest{
		ResourceName: w.resourceName,
		WriteOffset:  w.writeOffset,
		FinishWrite:  true,
	}); err != nil {
		w.cancel()
		w.client.CloseAndRecv()
		return err
	}
	_, err := w.client.CloseAndRecv()
	w.cancel()
	return err
}

func (ba *casWithZstdBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	if digest.GetSizeBytes() < ba.compressionThresholdBytes {
		return ba.uncompressedBlobAccess.Put(ctx, digest, b)
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	resourceName := digest.GetByteStreamWritePath(uuid.Must(ba.uuidGenerator()), remoteexecution.Compressor_ZSTD)
	client, err := ba.byteStreamClient.Write(
		metadata.AppendToOutgoingContext(ctxWithCancel, resourceNameHeader, resourceName),
	)
	if err != nil {
		cancel()
		return err
	}

	byteStreamWriter := &zstdByteStreamWriter{
		client:       client,
		resourceName: resourceName,
		writeOffset:  0,
		cancel:       cancel,
	}

	zstdWriter, err := zstd.NewWriter(byteStreamWriter, zstd.WithEncoderConcurrency(1))
	if err != nil {
		cancel()
		client.CloseAndRecv()
		return status.Errorf(codes.Internal, "Failed to create zstd writer: %v", err)
	}

	if err := b.IntoWriter(zstdWriter); err != nil {
		zstdWriter.Close()
		byteStreamWriter.Close()
		return err
	}

	if err := zstdWriter.Close(); err != nil {
		byteStreamWriter.Close()
		return err
	}

	return byteStreamWriter.Close()
}

func (ba *casWithZstdBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.uncompressedBlobAccess.FindMissing(ctx, digests)
}

func (ba *casWithZstdBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	cacheCapabilities, err := getCacheCapabilities(ctx, ba.capabilitiesClient, instanceName)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(cacheCapabilities.SupportedCompressors, remoteexecution.Compressor_ZSTD) {
		return nil, status.Error(codes.FailedPrecondition, "Server does not support ZSTD compression")
	}

	// Only return fields that pertain to the Content Addressable
	// Storage. Include compression support information.
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions:      digest.RemoveUnsupportedDigestFunctions(cacheCapabilities.DigestFunctions),
			SupportedCompressors: cacheCapabilities.SupportedCompressors,
		},
	}, nil
}
