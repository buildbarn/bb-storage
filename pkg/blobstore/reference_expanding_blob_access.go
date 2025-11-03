package blobstore

import (
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	cloud_aws "github.com/buildbarn/bb-storage/pkg/cloud/aws"
	cloud_gcp "github.com/buildbarn/bb-storage/pkg/cloud/gcp"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/klauspost/compress/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type referenceExpandingBlobAccess struct {
	indirectContentAddressableStorage BlobAccess
	contentAddressableStorage         BlobAccess
	httpClient                        *http.Client
	s3Client                          cloud_aws.S3Client
	gcsClient                         cloud_gcp.StorageClient
	maximumMessageSizeBytes           int
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
func NewReferenceExpandingBlobAccess(indirectContentAddressableStorage, contentAddressableStorage BlobAccess, httpClient *http.Client, s3Client cloud_aws.S3Client, gcsClient cloud_gcp.StorageClient, maximumMessageSizeBytes int) BlobAccess {
	return &referenceExpandingBlobAccess{
		indirectContentAddressableStorage: indirectContentAddressableStorage,
		contentAddressableStorage:         contentAddressableStorage,
		httpClient:                        httpClient,
		s3Client:                          s3Client,
		gcsClient:                         gcsClient,
		maximumMessageSizeBytes:           maximumMessageSizeBytes,
	}
}

func (ba *referenceExpandingBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	// Load reference from the ICAS.
	referenceMessage, err := ba.indirectContentAddressableStorage.Get(ctx, blobDigest).ToProto(&icas.Reference{}, ba.maximumMessageSizeBytes)
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
			return buffer.NewBufferFromError(util.StatusWrap(errToStatus(err), "HTTP request failed"))
		}
		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return buffer.NewBufferFromError(status.Errorf(codes.Internal, "HTTP request failed with status %#v", resp.Status))
		}
		r = resp.Body
	case *icas.Reference_S3_:
		// Download the object from S3.
		getObjectOutput, err := ba.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(medium.S3.Bucket),
			Key:    aws.String(medium.S3.Key),
			Range:  aws.String(getHTTPRangeHeader(reference)),
		})
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrap(errToStatus(err), "S3 request failed"))
		}
		r = getObjectOutput.Body
	case *icas.Reference_Gcs:
		if ba.gcsClient == nil {
			return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "No Google Cloud Storage client configured"))
		}

		// Download the object from Google Cloud Storage.
		sizeBytes := cloud_gcp.ReadUntilEOF
		if reference.SizeBytes > 0 {
			sizeBytes = reference.SizeBytes
		}
		r, err = ba.gcsClient.
			Bucket(medium.Gcs.Bucket).
			Object(medium.Gcs.Object).
			NewRangeReader(ctx, reference.OffsetBytes, sizeBytes)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrap(errToStatus(err), "Google Cloud Storage request failed"))
		}
	case *icas.Reference_ContentAddressableStorage_:
		if reference.OffsetBytes != 0 || reference.SizeBytes != 0 {
			return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "Partial reads are not supported by the Content Addressable Storage backend"))
		}

		instanceNameStr := medium.ContentAddressableStorage.InstanceName
		instanceName, err := digest.NewInstanceName(instanceNameStr)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapfWithCode(err, codes.Internal, "Invalid instance name %#v", instanceNameStr))
		}
		digestFunctionValue := medium.ContentAddressableStorage.DigestFunction
		digestFunction, err := instanceName.GetDigestFunction(digestFunctionValue, 0)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapfWithCode(err, codes.Internal, "Invalid digest function %d", digestFunctionValue))
		}
		referenceDigest, err := digestFunction.NewDigestFromProto(medium.ContentAddressableStorage.BlobDigest)
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "Invalid digest"))
		}

		b := ba.contentAddressableStorage.Get(ctx, referenceDigest)
		if reference.Decompressor == remoteexecution.Compressor_IDENTITY {
			// Optimize the fast path: if no transformations are
			// being performed and the digests are identical, we
			// can pass through the underlying buffer directly.
			instanceNamePatcher := digest.NewInstanceNamePatcher(referenceDigest.GetInstanceName(), blobDigest.GetInstanceName())
			if blobDigest == instanceNamePatcher.PatchDigest(referenceDigest) {
				return b
			}
		}
		r = b.ToReader()
	default:
		return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "Reference uses an unsupported medium"))
	}

	r = statusReturningReadCloser{r: r}

	// Apply a decompressor if needed.
	switch reference.Decompressor {
	case remoteexecution.Compressor_IDENTITY:
	case remoteexecution.Compressor_ZSTD:
		// Disable concurrency, as the default is to use
		// GOMAXPROCS. We should just use a single thread,
		// because many BlobAccess operations may run in
		// parallel.
		decoder, err := util.NewZstdReadCloser(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true))
		if err != nil {
			r.Close()
			return buffer.NewBufferFromError(util.StatusWrapWithCode(err, codes.Internal, "Failed to create Zstandard decoder"))
		}
		r = decoder
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
	return buffer.NewCASBufferFromReader(blobDigest, r, buffer.BackendProvided(buffer.Irreparable(blobDigest)))
}

func (ba *referenceExpandingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (referenceExpandingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return status.Error(codes.InvalidArgument, "The Indirect Content Addressable Storage can only store references, not data")
}

func (referenceExpandingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, status.Error(codes.InvalidArgument, "The Indirect Content Addressable Storage cannot be queried for capabilities")
}

func (ba *referenceExpandingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.indirectContentAddressableStorage.FindMissing(ctx, digests)
}

func errToStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Internal, err.Error())
}

// statusReturningReadCloser is a decorator for ReadCloser that
// transforms any errors returned by the underlying transport of an
// object retrieved through referenceExpandingBlobAccess gRPC style
// status.
type statusReturningReadCloser struct {
	r io.ReadCloser
}

func (r statusReturningReadCloser) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != io.EOF {
		err = errToStatus(err)
	}
	return n, err
}

func (r statusReturningReadCloser) Close() error {
	return errToStatus(r.r.Close())
}
