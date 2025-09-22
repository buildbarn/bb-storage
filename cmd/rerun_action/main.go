package main

import (
	"context"
	"log"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/rerun_action"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: rerun_action rerun_action.jsonnet")
		}
		var configuration rerun_action.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}

		grpcClientFactory := grpc.NewBaseClientFactory(grpc.BaseClientDialer, nil, nil, nil)

		grpcClient, err := grpcClientFactory.NewClientFromConfiguration(configuration.GrpcClient, dependenciesGroup)
		if err != nil {
			return util.StatusWrap(err, "Failed to create gRPC client")
		}
		executionClient := remoteexecution.NewExecutionClient(grpcClient)

		stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
			InstanceName:    configuration.InstanceName,
			SkipCacheLookup: true,
			ActionDigest:    configuration.ActionDigest,
			DigestFunction:  configuration.DigestFunction,
		})
		if err != nil {
			return util.StatusWrap(err, "Failed to execute")
		}
		for {
			response, err := stream.Recv()
			if err != nil {
				return util.StatusWrap(err, "Failed to receive response")
			}
			var executeResponse remoteexecution.ExecuteResponse
			if response.GetResponse().UnmarshalTo(&executeResponse) == nil {
				if executeResponse.Result != nil {
					executeResponse.Result.ExecutionMetadata = nil
				}
				log.Print(protojson.Format(&executeResponse))
				return nil
			}
			log.Print(protojson.Format(response))
		}
	})
}
