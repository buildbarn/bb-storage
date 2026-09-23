package referenceexpanding

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
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	cloud_aws "github.com/buildbarn/bb-storage/pkg/cloud/aws"
	cloud_gcp "github.com/buildbarn/bb-storage/pkg/cloud/gcp"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type referenceExpandingBlobAccess struct {
	indirectContentAddressableStorage blobstore.BlobAccess[*icas.Reference]
	chunkBytesReader                  reader.Reader[[]byte]
	chunkListFetcher                  chunk.ListFetcher
	cdcParametersFetcher              capabilities.CDCParametersFetcher
	httpClient                        *http.Client
	s3Client                          cloud_aws.S3Client
	gcsClient                         cloud_gcp.StorageClient
	maximumMessageSizeBytes           int
	zstdPool                          bb_zstd.Pool
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
func NewReferenceExpandingBlobAccess(indirectContentAddressableStorage blobstore.BlobAccess[*icas.Reference], chunkBytesReader reader.Reader[[]byte], chunkListFetcher chunk.ListFetcher, cdcParametersFetcher capabilities.CDCParametersFetcher, httpClient *http.Client, s3Client cloud_aws.S3Client, gcsClient cloud_gcp.StorageClient, maximumMessageSizeBytes int, zstdPool bb_zstd.Pool) blobstore.BlobAccess[*chunk.Chunk] {
	return &referenceExpandingBlobAccess{
		indirectContentAddressableStorage: indirectContentAddressableStorage,
		chunkBytesReader:                  chunkBytesReader,
		chunkListFetcher:                  chunkListFetcher,
		cdcParametersFetcher:              cdcParametersFetcher,
		httpClient:                        httpClient,
		s3Client:                          s3Client,
		gcsClient:                         gcsClient,
		zstdPool:                          zstdPool,
		maximumMessageSizeBytes:           maximumMessageSizeBytes,
	}
}

func (ba *referenceExpandingBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) (*chunk.Chunk, error) {
	// Check that the reference refers to something that can be expanded
	// into a chunk.
	if blobDigest.GetSizeBytes() > int64(ba.maximumMessageSizeBytes) {
		return nil, status.Errorf(codes.InvalidArgument, "Reference size %d exceeds maximum message size %d", blobDigest.GetSizeBytes(), ba.maximumMessageSizeBytes)
	}

	// Load reference from the ICAS.
	reference, err := ba.indirectContentAddressableStorage.Get(ctx, blobDigest)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to load reference")
	}

	// Load the object from the appropriate data store.
	var r io.ReadCloser
	switch medium := reference.Medium.(type) {
	case *icas.Reference_HttpUrl:
		// Download the object through HTTP.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, medium.HttpUrl, nil)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to create HTTP request")
		}
		req.Header.Add("Range", getHTTPRangeHeader(reference))
		resp, err := ba.httpClient.Do(req)
		if err != nil {
			return nil, util.StatusWrap(errToStatus(err), "HTTP request failed")
		}
		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return nil, status.Errorf(codes.Internal, "HTTP request failed with status %#v", resp.Status)
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
			return nil, util.StatusWrap(errToStatus(err), "S3 request failed")
		}
		r = getObjectOutput.Body
	case *icas.Reference_Gcs:
		if ba.gcsClient == nil {
			return nil, status.Error(codes.Unimplemented, "No Google Cloud Storage client configured")
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
			return nil, util.StatusWrap(errToStatus(err), "Google Cloud Storage request failed")
		}
	case *icas.Reference_ContentAddressableStorage_:
		if reference.OffsetBytes != 0 || reference.SizeBytes != 0 {
			return nil, status.Error(codes.Unimplemented, "Partial reads are not supported by the Content Addressable Storage backend")
		}

		instanceNameStr := medium.ContentAddressableStorage.InstanceName
		instanceName, err := digest.NewInstanceName(instanceNameStr)
		if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Invalid instance name %#v", instanceNameStr)
		}
		digestFunctionValue := medium.ContentAddressableStorage.DigestFunction
		digestFunction, err := instanceName.GetDigestFunction(digestFunctionValue, 0)
		if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Invalid digest function %d", digestFunctionValue)
		}
		referenceDigest, err := digestFunction.NewDigestFromProto(medium.ContentAddressableStorage.BlobDigest)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.Internal, "Invalid digest")
		}

		params, err := ba.cdcParametersFetcher.FetchCDCParameters(ctx, referenceDigest.GetInstanceName())
		if err != nil {
			return nil, err
		}
		r, err = cas.GetReadCloserAt(ctx, ba.chunkBytesReader, ba.chunkListFetcher, params, referenceDigest, 0)
		if err != nil {
			return nil, err
		}
	default:
		return nil, status.Error(codes.Unimplemented, "Reference uses an unsupported medium")
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
		decoder, err := bb_zstd.NewReadCloser(ctx, ba.zstdPool, r)
		if err != nil {
			r.Close()
			return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to create Zstandard decoder")
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
		return nil, status.Error(codes.Unimplemented, "Reference uses an unsupported decompressor")
	}

	// TODO: Should we install a RepairFunc that deletes the ICAS
	// entry? That should likely only be done conditionally, as it
	// may not always be desirable to let clients mutate the ICAS.
	//
	// If we wanted to support this, should we add a separate
	// BlobAccess.Delete(), or maybe a mechanism to forward the
	// RepairFunc from the ICAS buffer?
	data := make([]byte, blobDigest.GetSizeBytes())
	if _, err = io.ReadFull(r, data); err != nil {
		r.Close()
		return nil, err
	}

	// Validate that the stream doesn't contain trailing garbage, and
	// that decompressors (like flate) cleanly hit EOF without truncation.
	var eofBuf [1]byte
	if n, err := r.Read(eofBuf[:]); n > 0 || err != io.EOF {
		r.Close()
		if err == nil {
			return nil, status.Errorf(codes.InvalidArgument, "Stream yielded more data than expected")
		}
		return nil, err
	}

	if err := r.Close(); err != nil {
		return nil, err
	}

	// Validate the checksum.
	digestGenerator := blobDigest.GetDigestFunction().NewGenerator(blobDigest.GetSizeBytes())
	digestGenerator.Write(data)
	computedDigest := digestGenerator.Sum()
	if blobDigest != computedDigest {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Buffer has checksum %s, while %s was expected",
			computedDigest.GetHashString(),
			blobDigest.GetHashString(),
		)
	}

	return chunk.NewChunk(ba.zstdPool, data), nil
}

func (referenceExpandingBlobAccess) Put(ctx context.Context, digest digest.Digest, value *chunk.Chunk) error {
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
