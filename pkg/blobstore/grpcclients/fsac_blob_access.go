package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fsacBlobAccess struct {
	filesystemAccessCacheClient fsac.FileSystemAccessCacheClient
	maximumMessageSizeBytes     int
}

// NewFSACBlobAccess creates a BlobAccess that relays any requests to a
// gRPC server that implements the fsac.FileSystemAccessCache service.
//
// This is a service that is specific to Buildbarn, used to store
// profiles of file system access patterns of build actions. These
// profiles can be used to perform readahead of objects stored in the
// action's input root.
func NewFSACBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &fsacBlobAccess{
		filesystemAccessCacheClient: fsac.NewFileSystemAccessCacheClient(client),
		maximumMessageSizeBytes:     maximumMessageSizeBytes,
	}
}

func (ba *fsacBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	digestFunction := digest.GetDigestFunction()
	filesystemAccessProfile, err := ba.filesystemAccessCacheClient.GetFileSystemAccessProfile(ctx, &fsac.GetFileSystemAccessProfileRequest{
		InstanceName:        digestFunction.GetInstanceName().String(),
		DigestFunction:      digestFunction.GetEnumValue(),
		ReducedActionDigest: digest.GetProto(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewProtoBufferFromProto(filesystemAccessProfile, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *fsacBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *fsacBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	filesystemAccessProfile, err := b.ToProto(&fsac.FileSystemAccessProfile{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return err
	}
	digestFunction := digest.GetDigestFunction()
	_, err = ba.filesystemAccessCacheClient.UpdateFileSystemAccessProfile(ctx, &fsac.UpdateFileSystemAccessProfileRequest{
		InstanceName:            digestFunction.GetInstanceName().String(),
		DigestFunction:          digestFunction.GetEnumValue(),
		ReducedActionDigest:     digest.GetProto(),
		FileSystemAccessProfile: filesystemAccessProfile.(*fsac.FileSystemAccessProfile),
	})
	return err
}

func (ba *fsacBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, status.Error(codes.Unimplemented, "File System Access Cache does not support bulk existence checking")
}

func (ba *fsacBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	panic("GetCapabilities() should only be called against BlobAccess instances for the Content Addressable Storage and Action Cache")
}
