package blobstore

import (
	"context"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HTTPClient is an interface around Go's standard HTTP client type. It
// has been added to aid unit testing.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

var _ HTTPClient = &http.Client{}

type referenceExpandingBlobAccess struct {
	blobAccess              BlobAccess
	httpClient              HTTPClient
	maximumMessageSizeBytes int
}

// NewReferenceExpandingBlobAccess takes an Indirect Content Addressable
// Storage (ICAS) backend and converts it to a Content Addressable
// Storage (CAS) backend. Any object requested through this BlobAccess
// will cause its reference to be loaded from the ICAS, followed by
// fetching its data from the referenced location.
func NewReferenceExpandingBlobAccess(blobAccess BlobAccess, httpClient HTTPClient, maximumMessageSizeBytes int) BlobAccess {
	return &referenceExpandingBlobAccess{
		blobAccess:              blobAccess,
		httpClient:              httpClient,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (ba *referenceExpandingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	// Load reference from the ICAS.
	reference, err := ba.blobAccess.Get(ctx, digest).ToProto(&icas.Reference{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to load reference"))
	}

	switch medium := reference.(*icas.Reference).Medium.(type) {
	case *icas.Reference_HttpUrl:
		// Download the object through HTTP.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, medium.HttpUrl, nil)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "Failed to create HTTP request"))
		}
		resp, err := ba.httpClient.Do(req)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "HTTP request failed"))
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return buffer.NewBufferFromError(status.Errorf(codes.Internal, "HTTP request failed with status %#v", resp.Status))
		}
		// TODO: Should we install a RepairStrategy that deletes
		// the ICAS entry? That should likely only be done
		// conditionally, as it may not always be desirable to
		// let clients mutate the ICAS.
		//
		// If we wanted to support this, should we add a
		// separate BlobAccess.Delete(), or maybe a mechanism to
		// forward the RepairStrategy from the ICAS buffer?
		return buffer.NewCASBufferFromReader(digest, resp.Body, buffer.Irreparable)
	default:
		return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "Reference uses an unsupported medium"))
	}
}

func (ba *referenceExpandingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return status.Error(codes.InvalidArgument, "The Indirect Content Addressable Storage can only store references, not data")
}

func (ba *referenceExpandingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.blobAccess.FindMissing(ctx, digests)
}
