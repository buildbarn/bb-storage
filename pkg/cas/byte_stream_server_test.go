package cas_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestExistenceByteStreamServer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Calls against underlying storage.
	blobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess.EXPECT().Get(gomock.Any(), util.MustNewDigest("", &remoteexecution.Digest{
		Hash:      "09f7e02f1290be211da707a266f153b3",
		SizeBytes: 5,
	})).Return(int64(5), ioutil.NopCloser(bytes.NewBufferString("Hello")), nil)
	blobAccess.EXPECT().Get(gomock.Any(), util.MustNewDigest("debian8", &remoteexecution.Digest{
		Hash:      "3538d378083b9afa5ffad767f7269509",
		SizeBytes: 22,
	})).Return(int64(22), ioutil.NopCloser(bytes.NewBufferString("This is a long message")), nil)
	blobAccess.EXPECT().Get(gomock.Any(), util.MustNewDigest("fedora28", &remoteexecution.Digest{
		Hash:      "09f34d28e9c8bb445ec996388968a9e8",
		SizeBytes: 7,
	})).Return(int64(0), nil, status.Error(codes.NotFound, "Blob not found"))

	blobAccess.EXPECT().Put(gomock.Any(), util.MustNewDigest("", &remoteexecution.Digest{
		Hash:      "94876e5b1ce62c7b2b5ff6e661624841",
		SizeBytes: 14,
	}), int64(14), gomock.Any()).DoAndReturn(func(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
		buf, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("LaputanMachine"), buf)
		require.NoError(t, r.Close())
		return nil
	})
	blobAccess.EXPECT().Put(gomock.Any(), util.MustNewDigest("", &remoteexecution.Digest{
		Hash:      "f10e562d8825ec2e17e0d9f58646f8084a658cfa",
		SizeBytes: 6,
	}), int64(6), gomock.Any()).DoAndReturn(func(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
		_, err := ioutil.ReadAll(r)
		s := status.Convert(err)
		require.Equal(t, codes.InvalidArgument, s.Code())
		require.Equal(t, "Client closed stream without finishing write", s.Message())
		require.NoError(t, r.Close())
		return err
	})
	blobAccess.EXPECT().Put(gomock.Any(), util.MustNewDigest("fedora28", &remoteexecution.Digest{
		Hash:      "cbd8f7984c654c25512e3d9241ae569f",
		SizeBytes: 3,
	}), int64(3), gomock.Any()).DoAndReturn(func(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
		_, err := ioutil.ReadAll(r)
		s := status.Convert(err)
		require.Equal(t, codes.InvalidArgument, s.Code())
		require.Equal(t, "Client closed stream twice", s.Message())
		require.NoError(t, r.Close())
		return err
	})
	blobAccess.EXPECT().Put(gomock.Any(), util.MustNewDigest("windows10", &remoteexecution.Digest{
		Hash:      "68e109f0f40ca72a15e05cc22786f8e6",
		SizeBytes: 10,
	}), int64(10), gomock.Any()).DoAndReturn(func(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
		_, err := ioutil.ReadAll(r)
		s := status.Convert(err)
		require.Equal(t, codes.InvalidArgument, s.Code())
		require.Equal(t, "Attempted to write at offset 4, while 5 was expected", s.Message())
		require.NoError(t, r.Close())
		return err
	})

	// Create an RPC server/client pair.
	l := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	bytestream.RegisterByteStreamServer(server, cas.NewByteStreamServer(blobAccess, 10))
	go func() {
		require.NoError(t, server.Serve(l))
	}()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
		return l.Dial()
	}), grpc.WithInsecure())
	require.NoError(t, err)
	defer server.Stop()
	defer conn.Close()
	client := bytestream.NewByteStreamClient(conn)

	// Attempt to access a bad resource name.
	req, err := client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: "This is an incorrect resource name",
	})
	require.NoError(t, err)
	_, err = req.Recv()
	s := status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Invalid resource naming scheme", s.Message())

	// Invalid digest length.
	req, err = client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: "blobs/cafebabe/12",
	})
	require.NoError(t, err)
	_, err = req.Recv()
	s = status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Unknown digest hash length: 8 characters", s.Message())

	// Non-lowercase xdigits in hash.
	req, err = client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: "blobs/89D5739BAABBBE65BE35CBE61C88E06D/12",
	})
	require.NoError(t, err)
	_, err = req.Recv()
	s = status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Non-hexadecimal character in digest hash: U+0044 'D'", s.Message())

	// Negative size in digest.
	req, err = client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: "blobs/e811818f80d9c3c22d577ba83d6196788e553bb408535bb42105cdff726a60ab/-42",
	})
	require.NoError(t, err)
	_, err = req.Recv()
	s = status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Invalid digest size: -42 bytes", s.Message())

	// Attempt to fetch the small blob without an instance name.
	req, err = client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: "blobs/09f7e02f1290be211da707a266f153b3/5",
	})
	require.NoError(t, err)
	readResponse, err := req.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), readResponse.Data)
	_, err = req.Recv()
	require.Equal(t, io.EOF, err)

	// Attempt to fetch the large blob with an instance name.
	req, err = client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: "debian8/blobs/3538d378083b9afa5ffad767f7269509/22",
	})
	require.NoError(t, err)
	readResponse, err = req.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("This is a "), readResponse.Data)
	readResponse, err = req.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("long messa"), readResponse.Data)
	readResponse, err = req.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("ge"), readResponse.Data)
	_, err = req.Recv()
	require.Equal(t, io.EOF, err)

	// Attempt to fetch a nonexistent blob.
	req, err = client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: "///fedora28//blobs/09f34d28e9c8bb445ec996388968a9e8/////7/",
	})
	require.NoError(t, err)
	_, err = req.Recv()
	s = status.Convert(err)
	require.Equal(t, codes.NotFound, s.Code())
	require.Equal(t, "Blob not found", s.Message())

	// Attempt to write to a bad resource name.
	stream, err := client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		ResourceName: "This is an incorrect resource name",
		Data:         []byte("Bleep bloop!"),
	}))
	_, err = stream.CloseAndRecv()
	s = status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Invalid resource naming scheme", s.Message())

	// Attempt to write a blob without an instance name.
	stream, err = client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		ResourceName: "uploads/7de747e0-ab6b-4d83-90cb-11989f84c473/blobs/94876e5b1ce62c7b2b5ff6e661624841/14",
		Data:         []byte("Laputan"),
	}))
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		Data:        []byte("Machine"),
		WriteOffset: 7,
		FinishWrite: true,
	}))
	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, int64(14), response.CommittedSize)

	// Attempt to write without finishing properly.
	stream, err = client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		ResourceName: "uploads/497a9982-9d2a-4a29-95b8-28bd971bce1d/blobs/f10e562d8825ec2e17e0d9f58646f8084a658cfa/6",
		Data:         []byte("Foo"),
	}))
	_, err = stream.CloseAndRecv()
	s = status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Client closed stream without finishing write", s.Message())

	// Attempted to write while finishing twice.
	stream, err = client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		ResourceName: "fedora28/uploads/d834d9c2-f3c9-4f30-a698-75fd4be9470d/blobs/cbd8f7984c654c25512e3d9241ae569f/3",
		Data:         []byte("Foo"),
		FinishWrite:  true,
	}))
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		Data:        []byte("Bar"),
		WriteOffset: 3,
		FinishWrite: true,
	}))
	_, err = stream.CloseAndRecv()
	s = status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Client closed stream twice", s.Message())

	// Attempted to write with a bad write offset.
	stream, err = client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		ResourceName: "windows10/uploads/d834d9c2-f3c9-4f30-a698-75fd4be9470d/blobs/68e109f0f40ca72a15e05cc22786f8e6/10",
		Data:         []byte("Hello"),
	}))
	require.NoError(t, stream.Send(&bytestream.WriteRequest{
		Data:        []byte("World"),
		WriteOffset: 4,
		FinishWrite: true,
	}))
	_, err = stream.CloseAndRecv()
	s = status.Convert(err)
	require.Equal(t, codes.InvalidArgument, s.Code())
	require.Equal(t, "Attempted to write at offset 4, while 5 was expected", s.Message())

	_, err = client.QueryWriteStatus(ctx, &bytestream.QueryWriteStatusRequest{
		ResourceName: "windows10/uploads/d834d9c2-f3c9-4f30-a698-75fd4be9470d/blobs/68e109f0f40ca72a15e05cc22786f8e6/10",
	})
	s = status.Convert(err)
	require.Equal(t, codes.Unimplemented, s.Code())
	require.Equal(t, "This service does not support querying write status", s.Message())
}
