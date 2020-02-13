package blobstore

import (
	"context"
	"fmt"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opencensus.io/trace"
)

type contentAddressableStorageBlobAccess struct {
	byteStreamClient                bytestream.ByteStreamClient
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	uuidGenerator                   util.UUIDGenerator
	readChunkSize                   int
}

// NewContentAddressableStorageBlobAccess creates a BlobAccess handle
// that relays any requests to a GRPC service that implements the
// bytestream.ByteStream and remoteexecution.ContentAddressableStorage
// services. Those are the services that Bazel uses to access blobs
// stored in the Content Addressable Storage.
func NewContentAddressableStorageBlobAccess(client *grpc.ClientConn, uuidGenerator util.UUIDGenerator, readChunkSize int) BlobAccess {
	return &contentAddressableStorageBlobAccess{
		byteStreamClient:                bytestream.NewByteStreamClient(client),
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		uuidGenerator:                   uuidGenerator,
		readChunkSize:                   readChunkSize,
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
}

func (ba *contentAddressableStorageBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	ctx, span := trace.StartSpan(ctx, "blobstore.ContentAddressableStorageBlobAccess.Get")
	defer span.End()

	var readRequest bytestream.ReadRequest
	if instance := digest.GetInstance(); instance == "" {
		readRequest.ResourceName = fmt.Sprintf("blobs/%s/%d", digest.GetHashString(), digest.GetSizeBytes())
	} else {
		readRequest.ResourceName = fmt.Sprintf("%s/blobs/%s/%d", instance, digest.GetHashString(), digest.GetSizeBytes())
	}
	ctxWithCancel, cancel := context.WithCancel(ctx)
	client, err := ba.byteStreamClient.Read(ctxWithCancel, &readRequest)
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewCASBufferFromChunkReader(digest, &byteStreamChunkReader{
		client: client,
		cancel: cancel,
	}, buffer.Irreparable)
}

func (ba *contentAddressableStorageBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	ctx, span := trace.StartSpan(ctx, "blobstore.ContentAddressableStorageBlobAccess.Put")
	defer span.End()

	r := b.ToChunkReader(0, ba.readChunkSize)
	defer r.Close()

	client, err := ba.byteStreamClient.Write(ctx)
	if err != nil {
		return err
	}

	var resourceName string
	if instance := digest.GetInstance(); instance == "" {
		resourceName = fmt.Sprintf("uploads/%s/blobs/%s/%d", uuid.Must(ba.uuidGenerator()), digest.GetHashString(), digest.GetSizeBytes())
	} else {
		resourceName = fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", instance, uuid.Must(ba.uuidGenerator()), digest.GetHashString(), digest.GetSizeBytes())
	}

	writeOffset := int64(0)
	for {
		if data, err := r.Read(); err == nil {
			// Non-terminating chunk.
			if err := client.Send(&bytestream.WriteRequest{
				ResourceName: resourceName,
				WriteOffset:  writeOffset,
				Data:         data,
			}); err != nil {
				return err
			}
			writeOffset += int64(len(data))
			resourceName = ""
		} else if err == io.EOF {
			// Terminating chunk.
			if err := client.Send(&bytestream.WriteRequest{
				ResourceName: resourceName,
				WriteOffset:  writeOffset,
				FinishWrite:  true,
			}); err != nil {
				return err
			}
			_, err := client.CloseAndRecv()
			return err
		} else {
			return err
		}
	}
}

func (ba *contentAddressableStorageBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ctx, span := trace.StartSpan(ctx, "blobstore.ContentAddressableStorageBlobAccess.FindMissing")
	defer span.End()

	// Convert digests to line format.
	if len(digests) == 0 {
		return nil, nil
	}
	instance := digests[0].GetInstance()
	request := remoteexecution.FindMissingBlobsRequest{
		InstanceName: instance,
	}
	for _, digest := range digests {
		if digest.GetInstance() != instance {
			return nil, status.Error(codes.InvalidArgument, "Cannot use mixed instance names in a single request")
		}
		request.BlobDigests = append(request.BlobDigests, digest.GetPartialDigest())
	}

	response, err := ba.contentAddressableStorageClient.FindMissingBlobs(ctx, &request)
	if err != nil {
		return nil, err
	}

	// Convert results back.
	outDigests := make([]*util.Digest, 0, len(response.MissingBlobDigests))
	for _, partialDigest := range response.MissingBlobDigests {
		digest, err := util.NewDigest(instance, partialDigest)
		if err != nil {
			return nil, err
		}
		outDigests = append(outDigests, digest)
	}
	return outDigests, nil
}
