package grpcservers

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type byteStreamServer struct {
	blobAccess    blobstore.BlobAccess
	readChunkSize int
}

// NewByteStreamServer creates a GRPC service for reading blobs from and
// writing blobs to a BlobAccess. It is used by Bazel to access the
// Content Addressable Storage (CAS).
func NewByteStreamServer(blobAccess blobstore.BlobAccess, readChunkSize int) bytestream.ByteStreamServer {
	return &byteStreamServer{
		blobAccess:    blobAccess,
		readChunkSize: readChunkSize,
	}
}

func (s *byteStreamServer) Read(in *bytestream.ReadRequest, out bytestream.ByteStream_ReadServer) error {
	if in.ReadLimit != 0 {
		return status.Error(codes.Unimplemented, "This service does not support downloading partial files")
	}
	digest, err := digest.NewDigestFromByteStreamReadPath(in.ResourceName)
	if err != nil {
		return err
	}

	r := s.blobAccess.Get(out.Context(), digest).ToChunkReader(in.ReadOffset, s.readChunkSize)
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

func (r *byteStreamWriteServerChunkReader) Close() {}

func (s *byteStreamServer) Write(stream bytestream.ByteStream_WriteServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	digest, err := digest.NewDigestFromByteStreamWritePath(request.ResourceName)
	if err != nil {
		return err
	}
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

func (s *byteStreamServer) QueryWriteStatus(ctx context.Context, in *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "This service does not support querying write status")
}
