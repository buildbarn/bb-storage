package blobstore

import (
	"compress/flate"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	cloud_aws "github.com/buildbarn/bb-storage/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/klauspost/compress/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type referenceExpandingBlobAccess struct {
	blobAccess              BlobAccess
	httpClient              bb_http.Client
	s3                      cloud_aws.S3
	maximumMessageSizeBytes int
}

// getHTTPRangeHeader creates a HTTP Range header based on the offset
// and size stored in an ICAS Reference.
func getHTTPRangeHeader(reference *icas.Reference) string {
	if sizeBytes := reference.SizeBytes; sizeBytes > 0 {
		return fmt.Sprintf("bytes=%d-%d", reference.OffsetBytes, reference.OffsetBytes+sizeBytes-1)
	}
	return fmt.Sprintf("bytes=%d-", reference.OffsetBytes)
}

// NewReferenceExpandingBlobAccess takes an Indirect Content Addressable
// Storage (ICAS) backend and converts it to a Content Addressable
// Storage (CAS) backend. Any object requested through this BlobAccess
// will cause its reference to be loaded from the ICAS, followed by
// fetching its data from the referenced location.
func NewReferenceExpandingBlobAccess(blobAccess BlobAccess, httpClient bb_http.Client, s3 cloud_aws.S3, maximumMessageSizeBytes int) BlobAccess {
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
	case remoteexecution.Compressor_IDENTITY:
	case remoteexecution.Compressor_ZSTD:
		// Disable concurrency, as the default is to use
		// GOMAXPROCS. We should just use a single thread,
		// because many BlobAccess operations may run in
		// parallel.
		decoder, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true))
		if err != nil {
			r.Close()
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "Failed to create Zstandard decoder"))
		}
		r = &zstdReader{
			Decoder:          decoder,
			underlyingReader: r,
		}
	case remoteexecution.Compressor_DEFLATE:
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

	// TODO: Should we install a RepairFunc that deletes the ICAS
	// entry? That should likely only be done conditionally, as it
	// may not always be desirable to let clients mutate the ICAS.
	//
	// If we wanted to support this, should we add a separate
	// BlobAccess.Delete(), or maybe a mechanism to forward the
	// RepairFunc from the ICAS buffer?
	return buffer.NewCASBufferFromReader(digest, r, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *referenceExpandingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return status.Error(codes.InvalidArgument, "The Indirect Content Addressable Storage can only store references, not data")
}

func (ba *referenceExpandingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.blobAccess.FindMissing(ctx, digests)
}

// zstdReader is a decorator for zstd.Decoder that ensures both the
// decoder and the underlying stream are closed upon completion.
type zstdReader struct {
	*zstd.Decoder
	underlyingReader io.Closer
}

func (r *zstdReader) Close() error {
	r.Decoder.Close()
	return r.underlyingReader.Close()
}
