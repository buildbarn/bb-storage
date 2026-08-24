package cas

import (
	"bytes"
	"context"
	"io"
	"runtime/debug"

	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// IsSingleChunk checks if a digest is represented by a single chunk.
func IsSingleChunk(params cdc.Parameters, d digest.Digest) bool {
	return d.GetSizeBytes() < 2*params.MinChunkSizeBytes
}

// GetBytes returns the bytes of a digest from the CAS as a byteslice.
func GetBytes(ctx context.Context, cas ContentAddressableStorage, d digest.Digest) ([]byte, error) {
	params, err := cas.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch CDC parameters")
	}
	if IsSingleChunk(params, d) {
		return cas.FetchChunk(ctx, d)
	}
	manifest, err := cas.GetManifest(ctx, d)
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch chunk list")
	}
	ret := make([]byte, d.GetSizeBytes())
	for _, chunk := range manifest {
		chunkBytes, err := cas.FetchChunk(ctx, chunk.Digest)
		if err != nil {
			return nil, util.StatusWrap(err, "Could not fetch chunk")
		}
		copy(ret[chunk.Offset:], chunkBytes)
	}
	return ret, nil
}

// GetReadCloser returns an io.ReadCloser that reads contents of a blob
// in CAS.
func GetReadCloser(ctx context.Context, cas ContentAddressableStorage, d digest.Digest) (io.ReadCloser, error) {
	return GetReadCloserAt(ctx, cas, d, 0)
}

// GetReadCloserAt returns an io.ReadCloser that reads contents of a
// blob in seeked to a specific offset.
func GetReadCloserAt(ctx context.Context, cas ContentAddressableStorage, d digest.Digest, offset int64) (io.ReadCloser, error) {
	if offset < 0 || offset > d.GetSizeBytes() {
		return nil, status.Errorf(codes.InvalidArgument, "Offset %d is outside of blob %s", offset, d)
	}

	params, err := cas.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch CDC parameters")
	}
	if IsSingleChunk(params, d) {
		chunk, err := cas.FetchChunk(ctx, d)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(chunk[offset:])), nil
	}

	manifest, err := cas.GetManifest(ctx, d)
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch chunk list")
	}
	index, chunkOffset := chunklist.FindChunkOffset(manifest, uint64(offset))
	if index >= len(manifest) {
		// Offset at the end of the blob.
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	r := chunklist.NewChunkConcatenatingReader(ctx, manifest[index:], cas)
	if chunkOffset > 0 {
		if _, err := io.CopyN(io.Discard, r, chunkOffset); err != nil {
			return nil, util.StatusWrap(err, "Failed to skip to read offset")
		}
	}
	return r, nil
}

// PutBytes inserts all chunks for a digest from it's raw bytes.
func PutBytes(ctx context.Context, cas ContentAddressableStorage, d digest.Digest, data []byte) error {
	params, err := cas.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Could not fetch CDC parameters")
	}
	if IsSingleChunk(params, d) {
		return cas.PutChunk(ctx, d, data)
	}
	return PutReader(ctx, cas, d, bytes.NewReader(data))
}

// PutReader inserts all chunks for a digest from an io.Reader.
func PutReader(ctx context.Context, cas ContentAddressableStorage, d digest.Digest, r io.Reader) error {
	params, err := cas.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Could not fetch CDC parameters")
	}

	digestFunction := d.GetDigestFunction()
	chunker := cdc.NewReaderChunker(digestFunction, r, params.MinChunkSizeBytes, params.HorizonSizeBytes)
	wholeGen := digestFunction.NewGenerator(d.GetSizeBytes())

	chunkList := make(chunklist.ChunkList, 0, d.GetSizeBytes()/params.MinChunkSizeBytes+1)
	var offset uint64
	for {
		chunk, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.StatusWrap(err, "Failed to chunk write stream")
		}

		if _, err := wholeGen.Write(chunk.Data); err != nil {
			return status.Error(codes.Internal, "Could not compute digest of blob")
		}

		if err := cas.PutChunk(ctx, chunk.Digest, chunk.Data); err != nil {
			return util.StatusWrap(err, "Failed to save chunk")
		}

		chunkList = append(chunkList, chunklist.Entry{Offset: offset, Digest: chunk.Digest})
		offset += uint64(chunk.Digest.GetSizeBytes())
	}

	// Verify the whole blob against the advertised digest.
	if actual := wholeGen.Sum(); actual != d {
		debug.PrintStack()
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch: advertised %s, actual %s", d, actual)
	}

	// A single chunk is the trivial case: it already lives in the
	// chunk storage and needs no chunk list.
	if len(chunkList) <= 1 {
		return nil
	}

	// We generated and validated this chunk list ourselves.
	ctx = cdc.NewContextWithChunkListValidationBypass(ctx)
	if err := cas.PutManifest(ctx, d, chunkList); err != nil {
		return util.StatusWrap(err, "Could not save chunk list for blob")
	}
	return nil
}

// GetProto retrieves a proto message from the CAS unmarshalling it into
// the supplied message object
func GetProto[T proto.Message](ctx context.Context, cas ContentAddressableStorage, d digest.Digest, message T) (T, error) {
	var zero T
	bytes, err := GetBytes(ctx, cas, d)
	if err != nil {
		return zero, err
	}
	err = proto.Unmarshal(bytes, message)
	if err != nil {
		return zero, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to unmarshal message")
	}
	return message, nil
}

// IntoWriter streams the chunks of a blob directly into the provided
// io.Writer from the specified offset.
func IntoWriter(ctx context.Context, cas ContentAddressableStorage, d digest.Digest, offset int64, w io.Writer) error {
	if offset < 0 || offset > d.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "Invalid offset %d for digest %s", offset, d)
	}

	params, err := cas.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return util.StatusWrapf(err, "Failed to fetch CDC parameters for instance %s", d.GetInstanceName())
	}

	if IsSingleChunk(params, d) {
		chunkData, err := cas.FetchChunk(ctx, d)
		if err != nil {
			return err
		}
		if _, err := w.Write(chunkData[offset:]); err != nil {
			return err
		}
		return nil
	}

	manifest, err := cas.GetManifest(ctx, d)
	if err != nil {
		return err
	}
	index, chunkOffset := chunklist.FindChunkOffset(manifest, uint64(offset))
	for ; index < len(manifest); index++ {
		chunk := manifest[index]
		chunkData, err := cas.FetchChunk(ctx, chunk.Digest)
		if err != nil {
			return err
		}

		if _, err := w.Write(chunkData[chunkOffset:]); err != nil {
			return err
		}
		chunkOffset = 0
	}

	return nil
}
