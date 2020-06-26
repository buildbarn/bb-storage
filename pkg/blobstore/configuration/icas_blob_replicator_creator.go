package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type icasBlobReplicatorCreator struct{}

func (brc icasBlobReplicatorCreator) GetDigestKeyFormat() digest.KeyFormat {
	return digest.KeyWithoutInstance
}

func (brc icasBlobReplicatorCreator) NewCustomBlobReplicator(configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink blobstore.BlobAccess) (mirrored.BlobReplicator, error) {
	return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
}

// ICASBlobReplicatorCreator is a BlobReplicatorCreator that can be
// provided to NewBlobReplicatorFromConfiguration() to construct a
// BlobReplicator that is suitable for replicating Indirect Content
// Addressable Storage objects.
var ICASBlobReplicatorCreator BlobReplicatorCreator = icasBlobReplicatorCreator{}
