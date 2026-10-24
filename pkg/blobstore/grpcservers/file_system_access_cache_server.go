package grpcservers

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/emptypb"
)

type fileSystemAccessCacheServer struct {
	blobAccess blobstore.BlobAccess[*fsac.FileSystemAccessProfile]
}

// NewFileSystemAccessCacheServer creates a gRPC service for serving the
// contents of an File System Access Cache (FSAC). The FSAC is a service
// that is specific to Buildbarn, used to store profiles of file system
// access patterns of build actions. These profiles can be used to
// perform readahead of objects stored in the action's input root.
func NewFileSystemAccessCacheServer(blobAccess blobstore.BlobAccess[*fsac.FileSystemAccessProfile]) fsac.FileSystemAccessCacheServer {
	return &fileSystemAccessCacheServer{
		blobAccess: blobAccess,
	}
}

func (s *fileSystemAccessCacheServer) GetFileSystemAccessProfile(ctx context.Context, in *fsac.GetFileSystemAccessProfileRequest) (*fsac.FileSystemAccessProfile, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, 0)
	if err != nil {
		return nil, err
	}

	digest, err := digestFunction.NewDigestFromProto(in.ReducedActionDigest)
	if err != nil {
		return nil, err
	}
	return s.blobAccess.Get(ctx, digest)
}

func (s *fileSystemAccessCacheServer) UpdateFileSystemAccessProfile(ctx context.Context, in *fsac.UpdateFileSystemAccessProfileRequest) (*emptypb.Empty, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, 0)
	if err != nil {
		return nil, err
	}

	digest, err := digestFunction.NewDigestFromProto(in.ReducedActionDigest)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, s.blobAccess.Put(
		ctx,
		digest,
		in.FileSystemAccessProfile,
	)
}
