package grpcservers

import (
	"context"
	"errors"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type byteStreamServer struct {
	blobAccess    blobstore.BlobAccess
	readChunkSize int
	zstdPool      bb_zstd.Pool
}

// NewByteStreamServer creates a GRPC service for reading blobs from and
// writing blobs to a BlobAccess. It is used by Bazel to access the
// Content Addressable Storage (CAS).
func NewByteStreamServer(blobAccess blobstore.BlobAccess, readChunkSize int, zstdPool bb_zstd.Pool) bytestream.ByteStreamServer {
	return &byteStreamServer{
		blobAccess:    blobAccess,
		readChunkSize: readChunkSize,
		zstdPool:      zstdPool,
	}
}

func (s *byteStreamServer) Read(in *bytestream.ReadRequest, out bytestream.ByteStream_ReadServer) error {
	if in.ReadLimit != 0 {
		return status.Error(codes.Unimplemented, "This service does not support downloading partial files")
	}
	digest, compressor, err := digest.NewDigestFromByteStreamReadPath(in.ResourceName)
	if err != nil {
		return err
	}
	ctx := out.Context()
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		r := s.blobAccess.Get(ctx, digest).ToChunkReader(in.ReadOffset, s.readChunkSize)
		defer r.Close()

		for {
			readBuf, readErr := r.Read()
			if readErr == io.EOF {
				return nil
			}
			if readErr != nil {
				return readErr
			}
			if writeErr := out.Send(&bytestream.ReadResponse{Data: readBuf}); writeErr != nil {
				return writeErr
			}
		}

	case remoteexecution.Compressor_ZSTD:
		b := s.blobAccess.Get(ctx, digest)
		encoder, err := s.zstdPool.NewEncoder(ctx, &readStreamWriter{out: out})
		if err != nil {
			b.Discard()
			return status.Errorf(codes.ResourceExhausted, "Failed to acquire ZSTD encoder: %v", err)
		}
		defer encoder.Close()
		return b.IntoWriter(encoder)
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support downloading compression type: %s", compressor)
	}
}

// readStreamWriter adapts the ByteStream_ReadServer to an io.Writer.
type readStreamWriter struct {
	out bytestream.ByteStream_ReadServer
}

func (w *readStreamWriter) Write(p []byte) (int, error) {
	if err := w.out.Send(&bytestream.ReadResponse{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}

type byteStreamWriteServerChunkReader struct {
	stream        bytestream.ByteStream_WriteServer
	writeOffset   int64
	data          []byte
	finishedWrite bool
}

func (r *byteStreamWriteServerChunkReader) setRequest(request *bytestream.WriteRequest) error {
	if r.finishedWrite {
		return status.Error(codes.InvalidArgument, "Client closed stream twice")
	}
	if request.WriteOffset != r.writeOffset {
		return status.Errorf(codes.InvalidArgument, "Attempted to write at offset %d, while %d was expected", request.WriteOffset, r.writeOffset)
	}

	r.writeOffset += int64(len(request.Data))
	r.data = request.Data
	r.finishedWrite = request.FinishWrite
	return nil
}

func (r *byteStreamWriteServerChunkReader) Read() ([]byte, error) {
	// Read next chunk if no data is present.
	if len(r.data) == 0 {
		request, err := r.stream.Recv()
		if err != nil {
			if err == io.EOF && !r.finishedWrite {
				return nil, status.Error(codes.InvalidArgument, "Client closed stream without finishing write")
			}
			return nil, err
		}
		if err := r.setRequest(request); err != nil {
			return nil, err
		}
	}

	data := r.data
	r.data = nil
	return data, nil
}

func (byteStreamWriteServerChunkReader) Close() {}

func (s *byteStreamServer) Write(stream bytestream.ByteStream_WriteServer) error {
	request, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "Client closed stream without sending an initial request")
		}
		return err
	}
	digest, compressor, err := digest.NewDigestFromByteStreamWritePath(request.ResourceName)
	if err != nil {
		return err
	}
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return s.writeIdentity(stream, request, digest)
	case remoteexecution.Compressor_ZSTD:
		return s.writeZstd(stream, request, digest)
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support uploading compression type: %s", compressor)
	}
}

func (s *byteStreamServer) writeIdentity(stream bytestream.ByteStream_WriteServer, request *bytestream.WriteRequest, digest digest.Digest) error {
	r := &byteStreamWriteServerChunkReader{stream: stream}
	if err := r.setRequest(request); err != nil {
		return err
	}
	if err := s.blobAccess.Put(
		stream.Context(),
		digest,
		buffer.NewCASBufferFromChunkReader(digest, r, buffer.UserProvided)); err != nil {
		return err
	}
	return stream.SendAndClose(&bytestream.WriteResponse{
		CommittedSize: digest.GetSizeBytes(),
	})
}

type zstdWriteStreamReader struct {
	stream      bytestream.ByteStream_WriteServer
	nextOffset  int64
	finished    bool
	pendingData []byte
}

func (r *zstdWriteStreamReader) Read(p []byte) (n int, err error) {
	if len(r.pendingData) > 0 {
		n = copy(p, r.pendingData)
		r.pendingData = r.pendingData[n:]
		return n, nil
	}

	if r.finished {
		return 0, io.EOF
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

func (zstdWriteStreamReader) Close() error {
	return nil
}

func (s *byteStreamServer) writeZstd(stream bytestream.ByteStream_WriteServer, request *bytestream.WriteRequest, digest digest.Digest) error {
	ctx := stream.Context()
	streamReader := &zstdWriteStreamReader{
		stream:      stream,
		nextOffset:  int64(len(request.Data)),
		finished:    request.FinishWrite,
		pendingData: request.Data,
	}

	zstdReader, err := bb_zstd.NewReadCloser(ctx, s.zstdPool, streamReader)
	if err != nil {
		return status.Errorf(codes.ResourceExhausted, "Failed to acquire ZSTD decoder: %v", err)
	}
	defer zstdReader.Close()

	if err := s.blobAccess.Put(
		ctx,
		digest,
		buffer.NewCASBufferFromReader(digest, zstdReader, buffer.UserProvided)); err != nil {
		return err
	}
	return stream.SendAndClose(&bytestream.WriteResponse{
		CommittedSize: streamReader.nextOffset,
	})
}

func (byteStreamServer) QueryWriteStatus(ctx context.Context, in *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "This service does not support querying write status")
}
