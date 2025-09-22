package main

import (
	"context"
	"errors"
	"log"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/rerun_action"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

		contentAddressableStorage := grpcclients.NewCASBlobAccess(
			grpcClient,
			uuid.NewRandom,
			/* readChunkSize = */ 65536,
			zstd.NewPoolFromConfiguration(nil),
		)

		instanceName, err := digest.NewInstanceName(configuration.InstanceName)
		if err != nil {
			return util.StatusWrapf(err, "Invalid instance name %#v", configuration.InstanceName)
		}
		digestFunction, err := instanceName.GetDigestFunction(configuration.DigestFunction, 0)
		if err != nil {
			return util.StatusWrap(err, "Invalid digest function")
		}
		actionDigest, err := digestFunction.NewDigestFromProto(configuration.ActionDigest)
		if err != nil {
			return util.StatusWrap(err, "Invalid action digest")
		}

		originalAction, err := contentAddressableStorage.Get(ctx, actionDigest).ToProto(&remoteexecution.Action{}, int(configuration.MaximumMessageSizeBytes))
		if err != nil {
			return util.StatusWrap(err, "Failed to download Action message")
		}
		copiedAction := proto.CloneOf(originalAction.(*remoteexecution.Action))
		copiedAction.DoNotCache = true
		copiedActionDigest, err := blobstore.CASPutProto(ctx, contentAddressableStorage, copiedAction, digestFunction)
		if err != nil {
			return util.StatusWrap(err, "Failed to upload modified Action message")
		}

		for i := 0; i < 200; i++ {
			siblingsGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				for {
					stream, err := executionClient.Execute(ctx, &remoteexecution.ExecuteRequest{
						InstanceName:    copiedActionDigest.GetInstanceName().String(),
						SkipCacheLookup: true,
						ActionDigest:    copiedActionDigest.GetProto(),
						DigestFunction:  copiedActionDigest.GetDigestFunction().GetEnumValue(),
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
							if /*executeResponse.Status.GetCode() != 0 ||*/ executeResponse.Result.GetExitCode() != 0 {
								return errors.New("Execution failed")
							}
							break
						}
					}
				}
			})
		}
		return nil
	})
}
