package grpcservers

import (
	"context"
	"errors"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type byteStreamServer struct {
	contentAddressableStorage cdc.ContentAddressableStorage
	zstdPool                  bb_zstd.Pool
}

// NewByteStreamServer creates a GRPC service for reading blobs from and
// writing blobs to a BlobAccess. It is used by Bazel to access the
// Content Addressable Storage (CAS).
func NewByteStreamServer(contentAddressableStorage cdc.ContentAddressableStorage, zstdPool bb_zstd.Pool) bytestream.ByteStreamServer {
	return &byteStreamServer{
		contentAddressableStorage: contentAddressableStorage,
		zstdPool:                  zstdPool,
	}
}

func (s *byteStreamServer) Read(in *bytestream.ReadRequest, out bytestream.ByteStream_ReadServer) error {
	if in.ReadLimit != 0 {
		return status.Error(codes.Unimplemented, "This service does not support downloading partial files")
	}
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

	ctx := out.Context()
	var w io.Writer = readStreamWriter{out: out}
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
	case remoteexecution.Compressor_ZSTD:
		encoder, err := s.zstdPool.NewEncoder(ctx, w)
		if err != nil {
			return status.Errorf(codes.ResourceExhausted, "Failed to acquire ZSTD encoder: %v", err)
		}
		defer encoder.Close()
		w = encoder
		return cdc.IntoWriter(ctx, s.contentAddressableStorage, d, in.ReadOffset, encoder)
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support downloading compression type: %s", compressor)
	}
	return cdc.IntoWriter(ctx, s.contentAddressableStorage, d, in.ReadOffset, w)
}

// readStreamWriter adapts the ByteStream_ReadServer to an io.Writer.
type readStreamWriter struct {
	out bytestream.ByteStream_ReadServer
}

func (w readStreamWriter) Write(p []byte) (int, error) {
	if err := w.out.Send(&bytestream.ReadResponse{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
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

	if err := cdc.PutReader(ctx, s.contentAddressableStorage, d, r); err != nil {
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
