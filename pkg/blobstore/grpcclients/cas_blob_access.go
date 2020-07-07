package grpcclients

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

type casBlobAccess struct {
	byteStreamClient                bytestream.ByteStreamClient
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	uuidGenerator                   util.UUIDGenerator
	readChunkSize                   int
}

// NewCASBlobAccess creates a BlobAccess handle that relays any requests
// to a GRPC service that implements the bytestream.ByteStream and
// remoteexecution.ContentAddressableStorage services. Those are the
// services that Bazel uses to access blobs stored in the Content
// Addressable Storage.
func NewCASBlobAccess(client grpc.ClientConnInterface, uuidGenerator util.UUIDGenerator, readChunkSize int) blobstore.BlobAccess {
	return &casBlobAccess{
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

func (ba *casBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	client, err := ba.byteStreamClient.Read(ctxWithCancel, &bytestream.ReadRequest{
		ResourceName: digest.GetByteStreamReadPath(),
	})
	if err != nil {
		cancel()
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewCASBufferFromChunkReader(digest, &byteStreamChunkReader{
		client: client,
		cancel: cancel,
	}, buffer.Irreparable)
}

func (ba *casBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	r := b.ToChunkReader(0, ba.readChunkSize)
	defer r.Close()

	client, err := ba.byteStreamClient.Write(ctx)
	if err != nil {
		return err
	}

	resourceName := digest.GetByteStreamWritePath(uuid.Must(ba.uuidGenerator()))
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

func (ba *casBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Partition all digests by instance name, as the
	// FindMissingBlobs() RPC can only process digests for a single
	// instance.
	perInstanceDigests := map[digest.InstanceName][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		instanceName := digest.GetInstanceName()
		perInstanceDigests[instanceName] = append(perInstanceDigests[instanceName], digest.GetProto())
	}

	missingDigests := digest.NewSetBuilder()
	for instanceName, blobDigests := range perInstanceDigests {
		// Call FindMissingBlobs() for each instance.
		request := remoteexecution.FindMissingBlobsRequest{
			InstanceName: instanceName.String(),
			BlobDigests:  blobDigests,
		}
		response, err := ba.contentAddressableStorageClient.FindMissingBlobs(ctx, &request)
		if err != nil {
			return digest.EmptySet, err
		}

		// Convert results back.
		for _, proto := range response.MissingBlobDigests {
			blobDigest, err := instanceName.NewDigestFromProto(proto)
			if err != nil {
				return digest.EmptySet, err
			}
			missingDigests.Add(blobDigest)
		}
	}
	return missingDigests.Build(), nil
}
