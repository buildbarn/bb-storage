package blobstore

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionCacheBlobAccess struct {
	actionCacheClient remoteexecution.ActionCacheClient
}

// NewActionCacheBlobAccess creates a BlobAccess handle that relays any
// requests to a GRPC service that implements the
// remoteexecution.ActionCache service. That is the service that Bazel
// uses to access action results stored in the Action Cache.
func NewActionCacheBlobAccess(client *grpc.ClientConn) BlobAccess {
	return &actionCacheBlobAccess{
		actionCacheClient: remoteexecution.NewActionCacheClient(client),
	}
}

func (ba *actionCacheBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	actionResult, err := ba.actionCacheClient.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
		InstanceName: digest.GetInstance(),
		ActionDigest: digest.GetPartialDigest(),
	})
	if err != nil {
		return 0, nil, err
	}

	data, err := proto.Marshal(actionResult)
	if err != nil {
		return 0, nil, err
	}
	return int64(len(data)), ioutil.NopCloser(bytes.NewBuffer(data)), nil
}

func (ba *actionCacheBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	data, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return err
	}
	var actionResult remoteexecution.ActionResult
	if err := proto.Unmarshal(data, &actionResult); err != nil {
		return err
	}

	_, err = ba.actionCacheClient.UpdateActionResult(ctx, &remoteexecution.UpdateActionResultRequest{
		InstanceName: digest.GetInstance(),
		ActionDigest: digest.GetPartialDigest(),
		ActionResult: &actionResult,
	})
	return err
}

func (ba *actionCacheBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	return status.Error(codes.Unimplemented, "Bazel remote execution protocol does not support object deletion")
}

func (ba *actionCacheBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	return nil, status.Error(codes.Unimplemented, "Bazel action cache does not support bulk existence checking")
}
