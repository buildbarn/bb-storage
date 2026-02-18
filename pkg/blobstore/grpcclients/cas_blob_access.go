package grpcclients

import (
	"context"
	"io"
	"slices"
	"sync"
	"sync/atomic"

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

type casBlobAccess struct {
	byteStreamClient                bytestream.ByteStreamClient
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	capabilitiesClient              remoteexecution.CapabilitiesClient
	uuidGenerator                   util.UUIDGenerator
	readChunkSize                   int
	enableZSTDCompression           bool
	supportedCompressors            atomic.Pointer[[]remoteexecution.Compressor_Value]
}

// NewCASBlobAccess creates a BlobAccess handle that relays any requests
// to a gRPC service that implements the bytestream.ByteStream and
// remoteexecution.ContentAddressableStorage services. Those are the
// services that Bazel uses to access blobs stored in the Content
// Addressable Storage.
//
// If enableZSTDCompression is true, the client will use ZSTD compression
// for ByteStream operations if the server supports it.
func NewCASBlobAccess(client grpc.ClientConnInterface, uuidGenerator util.UUIDGenerator, readChunkSize int, enableZSTDCompression bool) blobstore.BlobAccess {
	return &casBlobAccess{
		byteStreamClient:                bytestream.NewByteStreamClient(client),
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		capabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		uuidGenerator:                   uuidGenerator,
		readChunkSize:                   readChunkSize,
		enableZSTDCompression:           enableZSTDCompression,
	}
}

type byteStreamChunkReader struct {
	client bytestream.ByteStream_ReadClient
	cancel context.CancelFunc
}

func (r *byteStreamChunkReader) Read() ([]byte, error) {
	chunk, err := r.client.Recv()
	if err != nil {
		return nil, err
	}
	return chunk.Data, nil
}

func (r *byteStreamChunkReader) Close() {
	r.cancel()
	for {
		if _, err := r.client.Recv(); err != nil {
			break
		}
	}
}

type zstdByteStreamChunkReader struct {
	client        bytestream.ByteStream_ReadClient
	cancel        context.CancelFunc
	zstdReader    io.ReadCloser
	readChunkSize int
	wg            sync.WaitGroup
}

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

const resourceNameHeader = "build.bazel.remote.execution.v2.resource-name"

// shouldUseZSTDCompression checks if ZSTD compression should be used.
// It ensures GetCapabilities has been called to negotiate compression support.
func (ba *casBlobAccess) shouldUseZSTDCompression(ctx context.Context, digest digest.Digest) (bool, error) {
	if !ba.enableZSTDCompression {
		return false, nil
	}

	supportedCompressors := ba.supportedCompressors.Load()
	if supportedCompressors == nil {
		// Call GetCapabilities to check server support.
		if _, err := ba.GetCapabilities(ctx, digest.GetDigestFunction().GetInstanceName()); err != nil {
			return false, err
		}
		supportedCompressors = ba.supportedCompressors.Load()
	}

	return slices.Contains(*supportedCompressors, remoteexecution.Compressor_ZSTD), nil
}

func (ba *casBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		return buffer.NewBufferFromError(err)
	}

	compressor := remoteexecution.Compressor_IDENTITY
	if useCompression {
		compressor = remoteexecution.Compressor_ZSTD
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	resourceName := digest.GetByteStreamReadPath(compressor)
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

	if useCompression {
		return buffer.NewCASBufferFromChunkReader(digest, &zstdByteStreamChunkReader{
			client:        client,
			cancel:        cancel,
			readChunkSize: ba.readChunkSize,
		}, buffer.BackendProvided(buffer.Irreparable(digest)))
	}

	return buffer.NewCASBufferFromChunkReader(digest, &byteStreamChunkReader{
		client: client,
		cancel: cancel,
	}, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *casBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *casBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		b.Discard()
		return err
	}

	compressor := remoteexecution.Compressor_IDENTITY
	if useCompression {
		compressor = remoteexecution.Compressor_ZSTD
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	resourceName := digest.GetByteStreamWritePath(uuid.Must(ba.uuidGenerator()), compressor)
	client, err := ba.byteStreamClient.Write(
		metadata.AppendToOutgoingContext(ctxWithCancel, resourceNameHeader, resourceName),
	)
	if err != nil {
		cancel()
		b.Discard()
		return err
	}

	if useCompression {
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

	// Non-compressed path
	r := b.ToChunkReader(0, ba.readChunkSize)
	defer r.Close()

	writeOffset := int64(0)
	for {
		if data, err := r.Read(); err == nil {
			// Non-terminating chunk.
			if client.Send(&bytestream.WriteRequest{
				ResourceName: resourceName,
				WriteOffset:  writeOffset,
				Data:         data,
			}) != nil {
				cancel()
				_, err := client.CloseAndRecv()
				return err
			}
			writeOffset += int64(len(data))
			resourceName = ""
		} else if err == io.EOF {
			// Terminating chunk.
			if client.Send(&bytestream.WriteRequest{
				ResourceName: resourceName,
				WriteOffset:  writeOffset,
				FinishWrite:  true,
			}) != nil {
				cancel()
				_, err := client.CloseAndRecv()
				return err
			}
			_, err := client.CloseAndRecv()
			cancel()
			return err
		} else if err != nil {
			cancel()
			client.CloseAndRecv()
			return err
		}
	}
}

func (ba *casBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return findMissingBlobsInternal(ctx, digests, ba.contentAddressableStorageClient)
}

func findMissingBlobsInternal(ctx context.Context, digests digest.Set, cas remoteexecution.ContentAddressableStorageClient) (digest.Set, error) {
	// Partition all digests by digest function, as the
	// FindMissingBlobs() RPC can only process digests for a single
	// instance name and digest function.
	perFunctionDigests := map[digest.Function][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		digestFunction := digest.GetDigestFunction()
		perFunctionDigests[digestFunction] = append(perFunctionDigests[digestFunction], digest.GetProto())
	}

	missingDigests := digest.NewSetBuilder()
	for digestFunction, blobDigests := range perFunctionDigests {
		// Call FindMissingBlobs() for each digest function.
		request := remoteexecution.FindMissingBlobsRequest{
			InstanceName:   digestFunction.GetInstanceName().String(),
			BlobDigests:    blobDigests,
			DigestFunction: digestFunction.GetEnumValue(),
		}
		response, err := cas.FindMissingBlobs(ctx, &request)
		if err != nil {
			return digest.EmptySet, err
		}

		// Convert results back.
		for _, proto := range response.MissingBlobDigests {
			blobDigest, err := digestFunction.NewDigestFromProto(proto)
			if err != nil {
				return digest.EmptySet, err
			}
			missingDigests.Add(blobDigest)
		}
	}
	return missingDigests.Build(), nil
}

func (ba *casBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	serverCapabilities, err := getServerCapabilitiesWithCacheCapabilities(ctx, ba.capabilitiesClient, instanceName)
	if err != nil {
		return nil, err
	}

	cacheCapabilities := serverCapabilities.CacheCapabilities

	// Store supported compressors for compression negotiation.
	ba.supportedCompressors.Store(&cacheCapabilities.SupportedCompressors)

	// Only return fields that pertain to the Content Addressable
	// Storage. Don't set 'max_batch_total_size_bytes', as we don't
	// issue batch operations.
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: digest.RemoveUnsupportedDigestFunctions(cacheCapabilities.DigestFunctions),
		},
		DeprecatedApiVersion: serverCapabilities.DeprecatedApiVersion,
		LowApiVersion:        serverCapabilities.LowApiVersion,
		HighApiVersion:       serverCapabilities.HighApiVersion,
	}, nil
}
