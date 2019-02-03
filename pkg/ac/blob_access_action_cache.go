package ac

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/proto"

	"google.golang.org/grpc/codes"
)

type blobAccessActionCache struct {
	blobAccess blobstore.BlobAccess
}

// NewBlobAccessActionCache creates an ActionCache object that reads and
// writes action cache entries from a BlobAccess based store.
func NewBlobAccessActionCache(blobAccess blobstore.BlobAccess) ActionCache {
	return &blobAccessActionCache{
		blobAccess: blobAccess,
	}
}

func (ac *blobAccessActionCache) GetActionResult(ctx context.Context, digest *util.Digest) (*remoteexecution.ActionResult, error) {
	_, r, err := ac.blobAccess.Get(ctx, digest)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return nil, err
	}
	var actionResult remoteexecution.ActionResult
	if err := proto.Unmarshal(data, &actionResult); err != nil {
		// Malformed data stored in the Action Cache. Attempt to
		// delete the data and report it as if absent.
		if err := ac.blobAccess.Delete(ctx, digest); err == nil {
			log.Printf("Successfully deleted corrupted blob %s", digest)
		} else {
			log.Printf("Failed to delete corrupted blob %s: %s", digest, err)
		}
		return nil, util.StatusWrapWithCode(err, codes.NotFound, "Failed to unmarshal message")
	}
	return &actionResult, nil
}

func (ac *blobAccessActionCache) PutActionResult(ctx context.Context, digest *util.Digest, result *remoteexecution.ActionResult) error {
	data, err := proto.Marshal(result)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal message")
	}
	return ac.blobAccess.Put(ctx, digest, int64(len(data)), ioutil.NopCloser(bytes.NewBuffer(data)))
}
