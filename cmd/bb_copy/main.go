package main

import (
	"context"
	"log"
	"os"

	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_copy"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
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
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_copy bb_copy.jsonnet")
	}
	var configuration bb_copy.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}

	grpcClientFactory := grpc.NewBaseClientFactory(grpc.BaseClientDialer, nil, nil)
	terminationContext, terminationCancel := context.WithCancel(context.Background())
	var terminationGroup errgroup.Group

	blobAccessCreator := blobstore_configuration.NewCASBlobAccessCreator(
		grpcClientFactory,
		int(configuration.MaximumMessageSizeBytes))
	source, err := blobstore_configuration.NewBlobAccessFromConfiguration(
		terminationContext,
		&terminationGroup,
		configuration.Source,
		blobAccessCreator)
	if err != nil {
		log.Fatal("Failed to create source: ", err)
	}
	sink, err := blobstore_configuration.NewBlobAccessFromConfiguration(
		terminationContext,
		&terminationGroup,
		configuration.Sink,
		blobAccessCreator)
	if err != nil {
		log.Fatal("Failed to create sink: ", err)
	}
	replicator, err := blobstore_configuration.NewBlobReplicatorFromConfiguration(
		configuration.Replicator,
		source.BlobAccess,
		sink,
		blobstore_configuration.NewCASBlobReplicatorCreator(grpcClientFactory))
	if err != nil {
		log.Fatal("Failed to create replicator: ", err)
	}
	nestedReplicator := replication.NewNestedBlobReplicator(
		replicator,
		sink.DigestKeyFormat,
		int(configuration.MaximumMessageSizeBytes))

	ctx := context.Background()
	instanceName, err := digest.NewInstanceName(configuration.InstanceName)
	if err != nil {
		log.Fatal("Invalid instance name: ", err)
	}
	digestFunction, err := instanceName.GetDigestFunction(configuration.DigestFunction, 0)
	if err != nil {
		log.Fatal("Invalid digest function: ", err)
	}

	// Enqueue objects for replication.
	for i, action := range configuration.Actions {
		actionDigest, err := digestFunction.NewDigestFromProto(action)
		if err != nil {
			log.Fatal("Invalid action digest at index %d: %s", i, err)
		}
		nestedReplicator.EnqueueAction(actionDigest)
	}
	for i, blob := range configuration.Blobs {
		blobDigest, err := digestFunction.NewDigestFromProto(blob)
		if err != nil {
			log.Fatal("Invalid blob digest at index %d: %s", i, err)
		}
		if err := replicator.ReplicateMultiple(ctx, blobDigest.ToSingletonSet()); err != nil {
			log.Fatalf("Failed to schedule replication of blob with digest %#v: %s", blobDigest.String(), err)
		}
	}
	for i, directory := range configuration.Directories {
		directoryDigest, err := digestFunction.NewDigestFromProto(directory)
		if err != nil {
			log.Fatal("Invalid directory digest at index %d: %s", i, err)
		}
		nestedReplicator.EnqueueDirectory(directoryDigest)
	}
	for i, tree := range configuration.Trees {
		treeDigest, err := digestFunction.NewDigestFromProto(tree)
		if err != nil {
			log.Fatalf("Invalid tree digest at index %d: %s", i, err)
		}
		nestedReplicator.EnqueueTree(treeDigest)
	}

	// Perform replication of nested objects.
	replicationGroup, replicationCtx := errgroup.WithContext(ctx)
	for i := int32(0); i < configuration.TraversalConcurrency; i++ {
		replicationGroup.Go(func() error {
			return nestedReplicator.Replicate(replicationCtx)
		})
	}
	if err := replicationGroup.Wait(); err != nil {
		log.Fatal("Failed to replicate nested objects: ", err)
	}

	// Finalize.
	log.Print("Copying completed. Shutting down storage gracefully.")
	terminationCancel()
	if err := terminationGroup.Wait(); err != nil {
		log.Fatal("Failed to shut down storage gracefully: ", err)
	}
}
