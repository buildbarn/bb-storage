package main

import (
	"context"
	"log"
	"os"

	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_replicator"
	replicator_pb "github.com/buildbarn/bb-storage/pkg/proto/replicator"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_replicator bb_replicator.jsonnet")
	}
	var configuration bb_replicator.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	diagnosticsServer, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	signalContext := global.InstallTerminationSignalHandler(context.Background())
	terminationGroup, terminationContext := errgroup.WithContext(signalContext)
	startupContext, cancelStartupCtx := context.WithCancel(terminationContext)
	errorChannel := make(chan error)
	terminationGroup.Go(func() error {
		select {
		case err := <-errorChannel:
			return err
		case <-terminationContext.Done():
			// Do not block termination.
			return nil
		}
	})

	go func() {
		errorChannel <- util.StatusWrap(
			diagnosticsServer.ServeAndWait(startupContext, terminationContext),
			"Diagnostics server")
	}()

	blobAccessCreator := blobstore_configuration.NewCASBlobAccessCreator(
		grpcClientFactory,
		int(configuration.MaximumMessageSizeBytes))
	source, err := blobstore_configuration.NewBlobAccessFromConfiguration(
		terminationContext,
		terminationGroup,
		configuration.Source,
		blobAccessCreator)
	if err != nil {
		log.Fatal("Failed to create source: ", err)
	}
	sink, err := blobstore_configuration.NewBlobAccessFromConfiguration(
		terminationContext,
		terminationGroup,
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

	go func() {
		errorChannel <- util.StatusWrap(
			bb_grpc.NewServersFromConfigurationAndServe(
				startupContext,
				terminationContext,
				configuration.GrpcServers,
				func(s grpc.ServiceRegistrar) {
					replicator_pb.RegisterReplicatorServer(s, replication.NewReplicatorServer(replicator))
				}),
			"gRPC server failure")
	}()

	cancelStartupCtx()
	if err := terminationGroup.Wait(); err != nil {
		log.Fatal(err)
	}
	os.Exit(global.ExitCodeInterrupted)
}
