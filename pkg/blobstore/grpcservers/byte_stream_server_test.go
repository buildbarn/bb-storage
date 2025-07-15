package grpcservers_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"go.uber.org/mock/gomock"
)

func TestByteStreamServer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Create an RPC server/client pair.
	l := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	blobAccess := mock.NewMockBlobAccess(ctrl)
	bytestream.RegisterByteStreamServer(server, grpcservers.NewByteStreamServer(blobAccess, 10))
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

	t.Run("ReadBadResourceName", func(t *testing.T) {
		// Attempt to access a bad resource name.
		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "This is an incorrect resource name",
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"), err)
	})

	t.Run("ReadInvalidDigestLength", func(t *testing.T) {
		// Invalid digest length.
		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "blobs/cafebabe/12",
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unsupported digest function \"cafebabe\""), err)
	})

	t.Run("ReadUppercaseDigest", func(t *testing.T) {
		// Non-lowercase xdigits in hash.
		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "blobs/89D5739BAABBBE65BE35CBE61C88E06D/12",
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Non-hexadecimal character in digest hash: U+0044 'D'"), err)
	})

	t.Run("ReadNegativeSizeInDigest", func(t *testing.T) {
		// Negative size in digest.
		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "blobs/e811818f80d9c3c22d577ba83d6196788e553bb408535bb42105cdff726a60ab/-42",
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid digest size: -42 bytes"), err)
	})

	t.Run("ReadSuccessEmptyInstance", func(t *testing.T) {
		// Attempt to fetch the small blob without an instance name.
		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "09f7e02f1290be211da707a266f153b3", 5),
		).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "blobs/09f7e02f1290be211da707a266f153b3/5",
		})
		require.NoError(t, err)
		readResponse, err := req.Recv()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), readResponse.Data)
		_, err = req.Recv()
		require.Equal(t, io.EOF, err)
	})

	t.Run("ReadSuccessNonEmptyInstance", func(t *testing.T) {
		// Attempt to fetch the large blob with an instance name.
		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("debian8", remoteexecution.DigestFunction_MD5, "3538d378083b9afa5ffad767f7269509", 22),
		).Return(buffer.NewValidatedBufferFromByteSlice([]byte("This is a long message")))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "debian8/blobs/3538d378083b9afa5ffad767f7269509/22",
		})
		require.NoError(t, err)
		readResponse, err := req.Recv()
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
	})

	t.Run("ReadZSTDCompression", func(t *testing.T) {
		// Test reading with ZSTD compression.
		originalData := []byte("This is a test message that should be compressed with ZSTD")
		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256, "8b2c3f8a9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f61", 58),
		).Return(buffer.NewValidatedBufferFromByteSlice(originalData))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "compressed-blobs/zstd/8b2c3f8a9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f61/58",
		})
		require.NoError(t, err)

		var compressedData []byte
		for {
			response, err := req.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			compressedData = append(compressedData, response.Data...)
		}

		decoder, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer decoder.Close()

		decompressedData, err := decoder.DecodeAll(compressedData, nil)
		require.NoError(t, err)
		require.Equal(t, originalData, decompressedData)
	})

	t.Run("ReadZSTDCompressionLargeData", func(t *testing.T) {
		// Test reading large data with ZSTD compression.
		originalData := make([]byte, 100000)
		for i := range originalData {
			originalData[i] = byte(i % 256)
		}

		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2", 100000),
		).Return(buffer.NewValidatedBufferFromByteSlice(originalData))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "compressed-blobs/zstd/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2/100000",
		})
		require.NoError(t, err)

		var compressedData []byte
		for {
			response, err := req.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			compressedData = append(compressedData, response.Data...)
		}

		decoder, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer decoder.Close()

		decompressedData, err := decoder.DecodeAll(compressedData, nil)
		require.NoError(t, err)
		require.Equal(t, originalData, decompressedData)

		// Compressed data should be smaller than original data.
		require.Less(t, len(compressedData), len(originalData))
	})

	t.Run("ReadUnsupportedCompression", func(t *testing.T) {
		// Test reading with unsupported compression type.
		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "compressed-blobs/gzip/8b2c3f8a9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f61/58",
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "Unsupported compression scheme \"gzip\""), err)
	})

	t.Run("ReadNegativeReadOffset", func(t *testing.T) {
		// Attempt to fetch a blob with a negative offset.
		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7),
		).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye")))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "ubuntu1804/blobs/6fc422233a40a75a1f028e11c3cd1140/7",
			ReadOffset:   -4,
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Negative read offset: -4"), err)
	})

	t.Run("ReadOffsetBeyondEnd", func(t *testing.T) {
		// Attempt to fetch a blob with a offset beyond the size
		// of the blob.
		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "ad3c8ac9eef32188da352082244b3598", 13),
		).Return(buffer.NewValidatedBufferFromByteSlice([]byte("short message")))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "ubuntu1804/blobs/ad3c8ac9eef32188da352082244b3598/13",
			ReadOffset:   100,
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Buffer is 13 bytes in size, while a read at offset 100 was requested"), err)
	})

	t.Run("ReadSuccessWithOffset", func(t *testing.T) {
		// Attempt to fetch a lblob with an instance name and offset.
		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "da39a3ee5e6b4b0d3255bfef95601890", 19),
		).Return(buffer.NewValidatedBufferFromByteSlice([]byte("This offset message")))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "ubuntu1804/blobs/da39a3ee5e6b4b0d3255bfef95601890/19",
			ReadOffset:   4,
		})
		require.NoError(t, err)
		readResponse, err := req.Recv()
		require.NoError(t, err)
		require.Equal(t, []byte(" offset me"), readResponse.Data)
		readResponse, err = req.Recv()
		require.NoError(t, err)
		require.Equal(t, []byte("ssage"), readResponse.Data)
		_, err = req.Recv()
		require.Equal(t, io.EOF, err)
	})

	t.Run("ReadNonexistentBlob", func(t *testing.T) {
		// Attempt to fetch a nonexistent blob.
		blobAccess.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("fedora28", remoteexecution.DigestFunction_MD5, "09f34d28e9c8bb445ec996388968a9e8", 7),
		).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))

		req, err := client.Read(ctx, &bytestream.ReadRequest{
			ResourceName: "///fedora28//blobs/09f34d28e9c8bb445ec996388968a9e8/////7/",
		})
		require.NoError(t, err)
		_, err = req.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("WriteMissingInitialRequest", func(t *testing.T) {
		// The client must send at least one ReadRequest.
		stream, err := client.Write(ctx)
		require.NoError(t, err)
		_, err = stream.CloseAndRecv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Client closed stream without sending an initial request"), err)
	})

	t.Run("WriteBadResourceName", func(t *testing.T) {
		// Attempt to write to a bad resource name.
		stream, err := client.Write(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			ResourceName: "This is an incorrect resource name",
			Data:         []byte("Bleep bloop!"),
		}))
		_, err = stream.CloseAndRecv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"), err)
	})

	t.Run("WriteSuccessEmptyInstance", func(t *testing.T) {
		// Attempt to write a blob without an instance name.
		blobAccess.EXPECT().Put(
			gomock.Any(),
			digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "581c1053f832a1c719fb6528a588ccfd", 14),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("LaputanMachine"), data)
			return nil
		})

		stream, err := client.Write(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			ResourceName: "uploads/7de747e0-ab6b-4d83-90cb-11989f84c473/blobs/581c1053f832a1c719fb6528a588ccfd/14",
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
	})

	t.Run("WriteZSTDDecompression", func(t *testing.T) {
		// Test writing with ZSTD decompression.
		originalData := []byte("This is a test message that should be compressed with ZSTD for upload")

		encoder, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressedData := encoder.EncodeAll(originalData, nil)
		encoder.Close()

		digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
		generator := digestFunction.NewGenerator(int64(len(originalData)))
		generator.Write(originalData)
		actualDigest := generator.Sum()

		blobAccess.EXPECT().Put(
			gomock.Any(),
			actualDigest,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, originalData, data)
			return nil
		})

		stream, err := client.Write(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			ResourceName: "uploads/7de747e0-ab6b-4d83-90cb-11989f84c473/compressed-blobs/zstd/" + actualDigest.GetHashString() + "/" + fmt.Sprintf("%d", len(originalData)),
			Data:         compressedData,
			FinishWrite:  true,
		}))
		response, err := stream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, int64(len(compressedData)), response.CommittedSize)
	})

	t.Run("WriteZSTDDecompressionChunked", func(t *testing.T) {
		// Test writing with ZSTD decompression in multiple chunks.
		originalData := []byte("This is a longer test message that should be compressed with ZSTD and sent in multiple chunks to test streaming decompression")

		encoder, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressedData := encoder.EncodeAll(originalData, nil)
		encoder.Close()

		digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
		generator := digestFunction.NewGenerator(int64(len(originalData)))
		generator.Write(originalData)
		actualDigest := generator.Sum()

		blobAccess.EXPECT().Put(
			gomock.Any(),
			actualDigest,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, originalData, data)
			return nil
		})

		stream, err := client.Write(ctx)
		require.NoError(t, err)

		chunk1 := compressedData[:len(compressedData)/2]
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			ResourceName: "uploads/7de747e0-ab6b-4d83-90cb-11989f84c473/compressed-blobs/zstd/" + actualDigest.GetHashString() + "/" + fmt.Sprintf("%d", len(originalData)),
			Data:         chunk1,
		}))

		chunk2 := compressedData[len(compressedData)/2:]
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			Data:        chunk2,
			WriteOffset: int64(len(chunk1)),
			FinishWrite: true,
		}))

		response, err := stream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, int64(len(compressedData)), response.CommittedSize)
	})

	t.Run("WriteZstdDecompressionLargeData", func(t *testing.T) {
		// Test writing large data with ZSTD decompression.
		originalData := make([]byte, 50000)
		for i := range originalData {
			originalData[i] = byte(i % 256)
		}

		encoder, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressedData := encoder.EncodeAll(originalData, nil)
		encoder.Close()

		digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
		generator := digestFunction.NewGenerator(int64(len(originalData)))
		generator.Write(originalData)
		actualDigest := generator.Sum()

		blobAccess.EXPECT().Put(
			gomock.Any(),
			actualDigest,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(100000)
			require.NoError(t, err)
			require.Equal(t, originalData, data)
			return nil
		})

		stream, err := client.Write(ctx)
		require.NoError(t, err)

		chunkSize := 1000
		currentOffset := 0
		for currentOffset < len(compressedData) {
			endOffset := currentOffset + chunkSize
			if endOffset > len(compressedData) {
				endOffset = len(compressedData)
			}

			chunk := compressedData[currentOffset:endOffset]
			isLast := endOffset == len(compressedData)

			var req *bytestream.WriteRequest
			if currentOffset == 0 {
				req = &bytestream.WriteRequest{
					ResourceName: "uploads/7de747e0-ab6b-4d83-90cb-11989f84c473/compressed-blobs/zstd/" + actualDigest.GetHashString() + "/" + fmt.Sprintf("%d", len(originalData)),
					Data:         chunk,
					WriteOffset:  int64(currentOffset),
					FinishWrite:  isLast,
				}
			} else {
				req = &bytestream.WriteRequest{
					Data:        chunk,
					WriteOffset: int64(currentOffset),
					FinishWrite: isLast,
				}
			}

			require.NoError(t, stream.Send(req))
			currentOffset = endOffset
		}

		response, err := stream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, int64(len(compressedData)), response.CommittedSize)
	})

	t.Run("WriteZSTDInvalidData", func(t *testing.T) {
		// Test writing with invalid ZSTD data.
		invalidData := []byte("This is not valid ZSTD compressed data")

		blobAccess.EXPECT().Put(
			gomock.Any(),
			digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256, "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5", 10),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			_, err := b.ToByteSlice(1000)
			require.Error(t, err)
			return err
		})

		stream, err := client.Write(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			ResourceName: "uploads/7de747e0-ab6b-4d83-90cb-11989f84c473/compressed-blobs/zstd/d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5/10",
			Data:         invalidData,
			FinishWrite:  true,
		}))
		_, err = stream.CloseAndRecv()
		require.Error(t, err)
	})

	t.Run("WriteZSTDEmptyData", func(t *testing.T) {
		// Test writing empty data with ZSTD decompression.
		originalData := []byte{}

		encoder, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressedData := encoder.EncodeAll(originalData, nil)
		encoder.Close()

		digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
		generator := digestFunction.NewGenerator(int64(len(originalData)))
		generator.Write(originalData)
		actualDigest := generator.Sum()

		blobAccess.EXPECT().Put(
			gomock.Any(),
			actualDigest,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, originalData, data)
			return nil
		})

		stream, err := client.Write(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			ResourceName: "uploads/7de747e0-ab6b-4d83-90cb-11989f84c473/compressed-blobs/zstd/" + actualDigest.GetHashString() + "/0",
			Data:         compressedData,
			FinishWrite:  true,
		}))
		response, err := stream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, int64(len(compressedData)), response.CommittedSize)
	})

	t.Run("WriteEmptyStreamError", func(t *testing.T) {
		// Test writing to an empty stream.
		stream, err := client.Write(ctx)
		require.NoError(t, err)
		_, err = stream.CloseAndRecv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Client closed stream without sending an initial request"), err)
	})

	t.Run("WriteSuccessWithoutFinish", func(t *testing.T) {
		// Attempt to write without finishing properly.
		blobAccess.EXPECT().Put(
			gomock.Any(),
			digest.MustNewDigest("", remoteexecution.DigestFunction_SHA1, "f10e562d8825ec2e17e0d9f58646f8084a658cfa", 6),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			_, err := b.ToByteSlice(100)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Client closed stream without finishing write"), err)
			return err
		})

		stream, err := client.Write(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{
			ResourceName: "uploads/497a9982-9d2a-4a29-95b8-28bd971bce1d/blobs/f10e562d8825ec2e17e0d9f58646f8084a658cfa/6",
			Data:         []byte("Foo"),
		}))
		_, err = stream.CloseAndRecv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Client closed stream without finishing write"), err)
	})

	t.Run("WriteFailFinishTwice", func(t *testing.T) {
		// Attempted to write while finishing twice.
		blobAccess.EXPECT().Put(
			gomock.Any(),
			digest.MustNewDigest("fedora28", remoteexecution.DigestFunction_MD5, "cbd8f7984c654c25512e3d9241ae569f", 3),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			_, err := b.ToByteSlice(100)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Client closed stream twice"), err)
			return err
		})

		stream, err := client.Write(ctx)
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
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Client closed stream twice"), err)
	})

	t.Run("WriteFailBadOffset", func(t *testing.T) {
		// Attempted to write with a bad write offset.
		blobAccess.EXPECT().Put(
			gomock.Any(),
			digest.MustNewDigest("windows10", remoteexecution.DigestFunction_MD5, "68e109f0f40ca72a15e05cc22786f8e6", 10),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			_, err := b.ToByteSlice(100)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attempted to write at offset 4, while 5 was expected"), err)
			return err
		})

		stream, err := client.Write(ctx)
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
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attempted to write at offset 4, while 5 was expected"), err)
	})

	t.Run("QueryWriteStatus", func(t *testing.T) {
		_, err := client.QueryWriteStatus(ctx, &bytestream.QueryWriteStatusRequest{
			ResourceName: "windows10/uploads/d834d9c2-f3c9-4f30-a698-75fd4be9470d/blobs/68e109f0f40ca72a15e05cc22786f8e6/10",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "This service does not support querying write status"), err)
	})
}
