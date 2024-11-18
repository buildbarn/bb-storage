package main

import (
	"context"
	"os"

	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_copy"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A generic utility for copying data between storage backends. This
// utility is similar to bb_replicator, in that it uses the
// BlobReplicator layer to copy data between backends.
//
// The difference is that bb_replicator accepts requests of objects to
// copy through gRPC, while this utility accepts a list of digests in
// its configuration file, terminating as soon as replication is
// completed.
//
// When used in combination with ZIPReadingBlobAccess and
// ZIPWritingBlobAccess, this tool can also be used to backup and
// restore parts of the Content Addressable Storage.

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_copy bb_copy.jsonnet")
		}
		var configuration bb_copy.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}

		grpcClientFactory := grpc.NewBaseClientFactory(grpc.BaseClientDialer, nil, nil)

		blobAccessCreator := blobstore_configuration.NewCASBlobAccessCreator(
			grpcClientFactory,
			int(configuration.MaximumMessageSizeBytes))
		source, err := blobstore_configuration.NewBlobAccessFromConfiguration(
			dependenciesGroup,
			configuration.Source,
			blobAccessCreator)
		if err != nil {
			return util.StatusWrap(err, "Failed to create source")
		}
		sink, err := blobstore_configuration.NewBlobAccessFromConfiguration(
			dependenciesGroup,
			configuration.Sink,
			blobAccessCreator)
		if err != nil {
			return util.StatusWrap(err, "Failed to create sink")
		}
		replicator, err := blobstore_configuration.NewBlobReplicatorFromConfiguration(
			configuration.Replicator,
			source.BlobAccess,
			sink,
			blobstore_configuration.NewCASBlobReplicatorCreator(grpcClientFactory),
			"cas",
		)
		if err != nil {
			return util.StatusWrap(err, "Failed to create replicator")
		}
		nestedReplicator := replication.NewNestedBlobReplicator(
			replicator,
			sink.DigestKeyFormat,
			int(configuration.MaximumMessageSizeBytes))

		instanceName, err := digest.NewInstanceName(configuration.InstanceName)
		if err != nil {
			return util.StatusWrap(err, "Invalid instance name")
		}
		digestFunction, err := instanceName.GetDigestFunction(configuration.DigestFunction, 0)
		if err != nil {
			return util.StatusWrap(err, "Invalid digest function")
		}

		// Enqueue objects for replication.
		for i, action := range configuration.Actions {
			actionDigest, err := digestFunction.NewDigestFromProto(action)
			if err != nil {
				return util.StatusWrapf(err, "Invalid action digest at index %d", i)
			}
			nestedReplicator.EnqueueAction(actionDigest)
		}
		for i, blob := range configuration.Blobs {
			blobDigest, err := digestFunction.NewDigestFromProto(blob)
			if err != nil {
				return util.StatusWrapf(err, "Invalid blob digest at index %d", i)
			}
			if err := replicator.ReplicateMultiple(ctx, blobDigest.ToSingletonSet()); err != nil {
				return util.StatusWrapf(err, "Failed to schedule replication of blob with digest %#v", blobDigest.String())
			}
		}
		for i, directory := range configuration.Directories {
			directoryDigest, err := digestFunction.NewDigestFromProto(directory)
			if err != nil {
				return util.StatusWrapf(err, "Invalid directory digest at index %d", i)
			}
			nestedReplicator.EnqueueDirectory(directoryDigest)
		}
		for i, tree := range configuration.Trees {
			treeDigest, err := digestFunction.NewDigestFromProto(tree)
			if err != nil {
				return util.StatusWrapf(err, "Invalid tree digest at index %d", i)
			}
			nestedReplicator.EnqueueTree(treeDigest)
		}

		// Perform replication of nested objects.
		for i := int32(0); i < configuration.TraversalConcurrency; i++ {
			siblingsGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				return nestedReplicator.Replicate(ctx)
			})
		}
		return nil
	})
}
