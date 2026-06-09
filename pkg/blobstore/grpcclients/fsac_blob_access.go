package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
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
func NewFSACBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess[*fsac.FileSystemAccessProfile] {
	return &fsacBlobAccess{
		filesystemAccessCacheClient: fsac.NewFileSystemAccessCacheClient(client),
		maximumMessageSizeBytes:     maximumMessageSizeBytes,
	}
}

func (ba *fsacBlobAccess) Get(ctx context.Context, digest digest.Digest) (*fsac.FileSystemAccessProfile, error) {
	digestFunction := digest.GetDigestFunction()
	return ba.filesystemAccessCacheClient.GetFileSystemAccessProfile(ctx, &fsac.GetFileSystemAccessProfileRequest{
		InstanceName:        digestFunction.GetInstanceName().String(),
		DigestFunction:      digestFunction.GetEnumValue(),
		ReducedActionDigest: digest.GetProto(),
	})
}

func (ba *fsacBlobAccess) Put(ctx context.Context, digest digest.Digest, value *fsac.FileSystemAccessProfile) error {
	digestFunction := digest.GetDigestFunction()
	_, err := ba.filesystemAccessCacheClient.UpdateFileSystemAccessProfile(ctx, &fsac.UpdateFileSystemAccessProfileRequest{
		InstanceName:            digestFunction.GetInstanceName().String(),
		DigestFunction:          digestFunction.GetEnumValue(),
		ReducedActionDigest:     digest.GetProto(),
		FileSystemAccessProfile: value,
	})
	return err
}

func (fsacBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, status.Error(codes.Unimplemented, "File System Access Cache does not support bulk existence checking")
}

func (fsacBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	panic("GetCapabilities() should only be called against BlobAccess instances for the Content Addressable Storage and Action Cache")
}
