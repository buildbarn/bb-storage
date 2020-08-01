package blobstore

import (
	"compress/flate"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	cloud_aws "github.com/buildbarn/bb-storage/pkg/cloud/aws"
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
	s3                      cloud_aws.S3
	maximumMessageSizeBytes int
}

// getHTTPRangeHeader creates a HTTP Range header based on the offset
// and size stored in an ICAS Reference.
func getHTTPRangeHeader(reference *icas.Reference) string {
	return fmt.Sprintf("bytes=%d-%d", reference.OffsetBytes, reference.OffsetBytes+reference.SizeBytes-1)
}

// NewReferenceExpandingBlobAccess takes an Indirect Content Addressable
// Storage (ICAS) backend and converts it to a Content Addressable
// Storage (CAS) backend. Any object requested through this BlobAccess
// will cause its reference to be loaded from the ICAS, followed by
// fetching its data from the referenced location.
func NewReferenceExpandingBlobAccess(blobAccess BlobAccess, httpClient HTTPClient, s3 cloud_aws.S3, maximumMessageSizeBytes int) BlobAccess {
	return &referenceExpandingBlobAccess{
		blobAccess:              blobAccess,
		httpClient:              httpClient,
		s3:                      s3,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (ba *referenceExpandingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	// Load reference from the ICAS.
	referenceMessage, err := ba.blobAccess.Get(ctx, digest).ToProto(&icas.Reference{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to load reference"))
	}
	reference := referenceMessage.(*icas.Reference)

	// Load the object from the appropriate data store.
	var r io.ReadCloser
	switch medium := reference.Medium.(type) {
	case *icas.Reference_HttpUrl:
		// Download the object through HTTP.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, medium.HttpUrl, nil)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "Failed to create HTTP request"))
		}
		req.Header.Add("Range", getHTTPRangeHeader(reference))
		resp, err := ba.httpClient.Do(req)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "HTTP request failed"))
		}
		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return buffer.NewBufferFromError(status.Errorf(codes.Internal, "HTTP request failed with status %#v", resp.Status))
		}
		r = resp.Body
	case *icas.Reference_S3_:
		// Download the object from S3.
		getObjectOutput, err := ba.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(medium.S3.Bucket),
			Key:    aws.String(medium.S3.Key),
			Range:  aws.String(getHTTPRangeHeader(reference)),
		})
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "S3 request failed"))
		}
		r = getObjectOutput.Body
	default:
		return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "Reference uses an unsupported medium"))
	}

	// Apply a decompressor if needed.
	switch reference.Decompressor {
	case icas.Reference_NONE:
	case icas.Reference_DEFLATE:
		r = struct {
			io.Reader
			io.Closer
		}{
			Reader: flate.NewReader(r),
			Closer: r,
		}
	default:
		r.Close()
		return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "Reference uses an unsupported decompressor"))
	}

	// TODO: Should we install a RepairStrategy that deletes the
	// ICAS entry? That should likely only be done conditionally, as
	// it may not always be desirable to let clients mutate the ICAS.
	//
	// If we wanted to support this, should we add a separate
	// BlobAccess.Delete(), or maybe a mechanism to forward the
	// RepairStrategy from the ICAS buffer?
	return buffer.NewCASBufferFromReader(digest, r, buffer.Irreparable)
}

func (ba *referenceExpandingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return status.Error(codes.InvalidArgument, "The Indirect Content Addressable Storage can only store references, not data")
}

func (ba *referenceExpandingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.blobAccess.FindMissing(ctx, digests)
}
