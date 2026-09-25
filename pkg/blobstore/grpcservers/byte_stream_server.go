package grpcservers

import (
	"bytes"
	"context"
	"errors"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type byteStreamServer struct {
	chunkStorage         blobstore.BlobAccess[*chunk.Chunk]
	chunkListStorage     blobstore.BlobAccess[chunk.List]
	cdcParametersFetcher capabilities.CDCParametersFetcher
	zstdPool             bb_zstd.Pool
}

// NewByteStreamServer creates a GRPC service for reading blobs from and
// writing blobs to the Chunk Storage (CS) and Chunk List Storage (CLS).
// It is used by Bazel to access the Content Addressable Storage (CAS).
func NewByteStreamServer(chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkListStorage blobstore.BlobAccess[chunk.List], cdcParametersFetcher capabilities.CDCParametersFetcher, zstdPool bb_zstd.Pool) bytestream.ByteStreamServer {
	return &byteStreamServer{
		chunkStorage:         chunkStorage,
		chunkListStorage:     chunkListStorage,
		cdcParametersFetcher: cdcParametersFetcher,
		zstdPool:             zstdPool,
	}
}

func (s *byteStreamServer) Read(in *bytestream.ReadRequest, out bytestream.ByteStream_ReadServer) error {
	d, compressor, err := digest.NewDigestFromByteStreamReadPath(in.ResourceName)
	if err != nil {
		return err
	}
	if in.ReadOffset < 0 {
		return status.Errorf(codes.InvalidArgument, "Negative read offset: %d", in.ReadOffset)
	}
	if in.ReadOffset > d.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a read at offset %d was requested", d.GetSizeBytes(), in.ReadOffset)
	}
	if in.ReadLimit != 0 {
		if compressor != remoteexecution.Compressor_IDENTITY {
			// REAPI requires non-zero read limits on compressed ByteStream
			// reads to be rejected with INVALID_ARGUMENT.
			// https://github.com/bazelbuild/remote-apis/blob/becdd8f9ff811df88a22d3eadd6341753d51d167/build/bazel/remote/execution/v2/remote_execution.proto#L313-L317
			return status.Error(codes.InvalidArgument, "Read limits are not permitted for compressed blobs")
		}
		return status.Error(codes.Unimplemented, "This service does not support downloading partial files")
	}
	if compressor != remoteexecution.Compressor_IDENTITY && compressor != remoteexecution.Compressor_ZSTD {
		return status.Errorf(codes.InvalidArgument, "This service does not support compression type: %s", compressor.String())
	}
	ctx := out.Context()
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Could not determine cdc parameters")
	}
	chunkList := chunk.List{}
	if !cas.IsSingleChunk(params, d) {
		chunkList, err = s.chunkListStorage.Get(ctx, d)
		if err != nil {
			return err
		}
	} else if d.GetSizeBytes() == 0 {
	} else {
		// Blobs that fit in a single chunk have no chunk lists in
		// storage, but one may be created trivially on the fly.
		chunkList = chunk.List{Digests: []digest.Digest{d}, Offsets: []uint64{0}}
	}
	i, chunkOffset := chunkList.FindChunkOffset(uint64(in.ReadOffset))
	for ; i < len(chunkList.Digests); i++ {
		chunk, err := s.chunkStorage.Get(ctx, chunkList.Digests[i])
		if err != nil {
			return err
		}
		if chunkOffset == 0 || compressor == remoteexecution.Compressor_IDENTITY {
			// ZSTD has the neat property that we can simply send
			// multiple ZSTD encoded chunks and their concatenated byte
			// representation is a valid ZSTD representation of their
			// concatenated underlying bytes.
			var data []byte
			switch compressor {
			case remoteexecution.Compressor_IDENTITY:
				data = chunk.GetBytes()
			case remoteexecution.Compressor_ZSTD:
				data, err = chunk.GetBytesCompressed(ctx)
			default:
				panic("Unsupported compression algorithm should not be reachable")
			}
			if err != nil {
				return err
			}
			out.SendMsg(&bytestream.ReadResponse{
				Data: data[chunkOffset:],
			})
		} else {
			// If we have a chunk offset and are sending back compressed
			// bytes we need to find the data based on that offset in
			// its decompressed form, then compress it again before
			// sending it back.
			data := chunk.GetBytes()
			data = data[chunkOffset:]
			var buf bytes.Buffer
			buf.Grow(len(data))
			encoder, err := s.zstdPool.NewEncoder(ctx, &buf)
			if err != nil {
				return err
			}
			if _, err := encoder.Write(data); err != nil {
				encoder.Close()
				return err
			}
			if err := encoder.Close(); err != nil {
				return err
			}
			out.SendMsg(&bytestream.ReadResponse{
				Data: buf.Bytes(),
			})
		}
		chunkOffset = 0
	}
	return nil
}

func (s *byteStreamServer) Write(stream bytestream.ByteStream_WriteServer) error {
	request, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "Client closed stream without sending an initial request")
		}
		return err
	}

	d, compressor, err := digest.NewDigestFromByteStreamWritePath(request.ResourceName)
	if err != nil {
		return err
	}

	ctx := stream.Context()
	rawReader := &writeStreamReader{
		stream:      stream,
		nextOffset:  int64(len(request.Data)),
		finished:    request.FinishWrite,
		pendingData: request.Data,
	}

	var r io.Reader
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		r = rawReader
	case remoteexecution.Compressor_ZSTD:
		zr, err := bb_zstd.NewReadCloser(ctx, s.zstdPool, rawReader)
		if err != nil {
			return util.StatusWrap(err, "Failed to acquire ZSTD decoder")
		}
		defer zr.Close()
		r = zr
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support uploading compression type: %s", compressor)
	}

	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Could not determine cdc parameters")
	}
	if err := cas.PutReader(ctx, s.zstdPool, s.chunkStorage, s.chunkListStorage, params, d, r); err != nil {
		return err
	}
	return stream.SendAndClose(&bytestream.WriteResponse{
		CommittedSize: rawReader.nextOffset,
	})
}

type writeStreamReader struct {
	stream      bytestream.ByteStream_WriteServer
	nextOffset  int64
	finished    bool
	pendingData []byte
}

func (r *writeStreamReader) Read(p []byte) (n int, err error) {
	if len(r.pendingData) > 0 {
		n = copy(p, r.pendingData)
		r.pendingData = r.pendingData[n:]
		return n, nil
	}

	if r.finished {
		// The client indicated the write was finished. Check for trailing garbage.
		req, err := r.stream.Recv()
		if err == nil {
			if req.FinishWrite {
				return 0, status.Error(codes.InvalidArgument, "Client closed stream twice")
			}
			return 0, status.Error(codes.InvalidArgument, "Client sent extra data after finishing write")
		}
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		return 0, err
	}

	req, err := r.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, status.Error(codes.InvalidArgument, "Client closed stream without finishing write")
		}
		return 0, err
	}

	if req.WriteOffset != r.nextOffset {
		return 0, status.Errorf(codes.InvalidArgument, "Attempted to write at offset %d, while %d was expected", req.WriteOffset, r.nextOffset)
	}

	r.nextOffset += int64(len(req.Data))
	r.finished = req.FinishWrite

	n = copy(p, req.Data)
	r.pendingData = req.Data[n:]
	return n, nil
}

func (writeStreamReader) Close() error {
	return nil
}

func (byteStreamServer) QueryWriteStatus(ctx context.Context, in *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "This service does not support querying write status")
}
