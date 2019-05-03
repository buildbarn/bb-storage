package cas

import (
	"context"
	"io"
	"log"
	"strconv"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// parseResourceNameWrite parses resource name strings in one of the following two forms:
//
// - uploads/${uuid}/blobs/${hash}/${size}
// - ${instance}/uploads/${uuid}/blobs/${hash}/${size}
//
// In the process, the hash, size and instance are extracted.
func parseResourceNameWrite(resourceName string) (*util.Digest, error) {
	fields := strings.FieldsFunc(resourceName, func(r rune) bool { return r == '/' })
	l := len(fields)
	if (l != 5 && l != 6) || fields[l-5] != "uploads" || fields[l-3] != "blobs" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	size, err := strconv.ParseInt(fields[l-1], 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	instance := ""
	if l == 6 {
		instance = fields[0]
	}
	return util.NewDigest(
		instance,
		&remoteexecution.Digest{
			Hash:      fields[l-2],
			SizeBytes: size,
		})
}

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
	log.Println("byteStreamServer.Read", in)
	if in.ReadOffset != 0 || in.ReadLimit != 0 {
		return status.Error(codes.Unimplemented, "This service does not support downloading partial files")
	}

	digest, err := util.NewDigestFromBytestreamPath(in.ResourceName)
	if err != nil {
		return err
	}
	_, r, err := s.blobAccess.Get(out.Context(), digest)
	if err != nil {
		return err
	}
	defer r.Close()

	for {
		readBuf := make([]byte, s.readChunkSize)
		n, err := r.Read(readBuf)
		if err != nil && err != io.EOF {
			return err
		}
		if n > 0 {
			if err := out.Send(&bytestream.ReadResponse{Data: readBuf[:n]}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}

type byteStreamWriteServerReader struct {
	stream        bytestream.ByteStream_WriteServer
	writeOffset   int64
	data          []byte
	finishedWrite bool
}

func (r *byteStreamWriteServerReader) setRequest(request *bytestream.WriteRequest) error {
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

func (r *byteStreamWriteServerReader) Read(p []byte) (int, error) {
	// Read next chunk if no data is present.
	if len(r.data) == 0 {
		request, err := r.stream.Recv()
		if err != nil {
			if err == io.EOF && !r.finishedWrite {
				return 0, status.Error(codes.InvalidArgument, "Client closed stream without finishing write")
			}
			return 0, err
		}
		if err := r.setRequest(request); err != nil {
			return 0, err
		}
	}

	// Copy data from previously read partial chunk.
	c := copy(p, r.data)
	r.data = r.data[c:]
	return c, nil
}

func (r *byteStreamWriteServerReader) Close() error {
	return nil
}

func (s *byteStreamServer) Write(stream bytestream.ByteStream_WriteServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	log.Println("byteStreamServer.Write", request)
	digest, err := parseResourceNameWrite(request.ResourceName)
	if err != nil {
		return err
	}
	r := &byteStreamWriteServerReader{stream: stream}
	if err := r.setRequest(request); err != nil {
		return err
	}
	sizeBytes := digest.GetSizeBytes()
	if err := s.blobAccess.Put(stream.Context(), digest, sizeBytes, r); err != nil {
		return err
	}
	return stream.SendAndClose(&bytestream.WriteResponse{
		CommittedSize: sizeBytes,
	})
}

func (s *byteStreamServer) QueryWriteStatus(ctx context.Context, in *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "This service does not support querying write status")
}
