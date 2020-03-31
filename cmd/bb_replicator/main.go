package main

import (
	"log"
	"net/http"
	"os"

	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_replicator"
	replicator_pb "github.com/buildbarn/bb-storage/pkg/proto/replicator"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/gorilla/mux"

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
	if err := global.ApplyConfiguration(configuration.Global); err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	source, err := blobstore_configuration.CreateCASBlobAccessObjectFromConfig(
		configuration.Source,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal("Failed to create source: ", err)
	}
	sink, err := blobstore_configuration.CreateCASBlobAccessObjectFromConfig(
		configuration.Sink,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal("Failed to create sink: ", err)
	}
	replicator, err := blobstore_configuration.CreateBlobReplicatorFromConfig(
		configuration.Replicator,
		source,
		sink,
		digest.KeyWithoutInstance)
	if err != nil {
		log.Fatal("Failed to create replicator: ", err)
	}

	go func() {
		log.Fatal(
			"gRPC server failure: ",
			bb_grpc.NewGRPCServersFromConfigurationAndServe(
				configuration.GrpcServers,
				func(s *grpc.Server) {
					replicator_pb.RegisterReplicatorServer(s, mirrored.NewReplicatorServer(replicator))
				}))
	}()

	// Web server for metrics and profiling.
	router := mux.NewRouter()
	util.RegisterAdministrativeHTTPEndpoints(router)
	log.Fatal(http.ListenAndServe(configuration.HttpListenAddress, router))
}
