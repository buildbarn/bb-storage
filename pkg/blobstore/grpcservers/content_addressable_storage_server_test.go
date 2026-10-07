package grpcservers_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	status_pb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestContentAddressableStorageServerBatchReadBlobsSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 123)
	digest2 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 234)
	digest3 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6", 345)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 123,
			},
			{
				Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
				SizeBytes: 234,
			},
			{
				Hash:      "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6",
				SizeBytes: 345,
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	singleChunkParameters := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("ubuntu1804")).Return(singleChunkParameters, nil).Times(1)
	a := make([]byte, 123)
	chunkStorage.EXPECT().Get(ctx, digest1).Return(chunk.NewChunk(zstdPool, a), nil)
	b := make([]byte, 234)
	chunkStorage.EXPECT().Get(ctx, digest2).Return(chunk.NewChunk(zstdPool, b), nil)
	chunkStorage.EXPECT().Get(ctx, digest3).Return(nil, status.Error(codes.NotFound, "The object you requested could not be found"))

	maximumMessageSizeBytes := 4 << 20
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000)

	response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchReadBlobsResponse{
		Responses: []*remoteexecution.BatchReadBlobsResponse_Response{
			{
				Digest: &remoteexecution.Digest{
					Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
					SizeBytes: 123,
				},
				Data: a,
			},
			{
				Digest: &remoteexecution.Digest{
					Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
					SizeBytes: 234,
				},
				Data: b,
			},
			{
				Digest: &remoteexecution.Digest{
					Hash:      "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6",
					SizeBytes: 345,
				},
				Status: &status_pb.Status{
					Code:    int32(codes.NotFound),
					Message: "The object you requested could not be found",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchReadBlobsFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 123,
			},
			{
				Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
				SizeBytes: 234,
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	maximumMessageSizeBytes := 200
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000)

	_, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attempted to read a total of at least 357 bytes, while a maximum of 200 bytes is permitted"), err)
}

func TestContentAddressableStorageServerBatchReadBlobsZSTD(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests:               []*remoteexecution.Digest{digest1.GetProto()},
		InstanceName:          "ubuntu1804",
		AcceptableCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	data := []byte("Hello")
	var compressed bytes.Buffer
	encoder, err := zstdPool.NewEncoder(ctx, &compressed)
	require.NoError(t, err)
	_, err = encoder.Write(data)
	require.NoError(t, err)
	require.NoError(t, encoder.Close())

	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(params, nil)
	chunkStorage.EXPECT().Get(ctx, digest1).Return(chunk.NewChunkWithCompressedData(data, compressed.Bytes()), nil)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000)

	response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchReadBlobsResponse{
		Responses: []*remoteexecution.BatchReadBlobsResponse_Response{
			{
				Digest:     digest1.GetProto(),
				Data:       compressed.Bytes(),
				Compressor: remoteexecution.Compressor_ZSTD,
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchUpdateBlobs(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	digest2 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "f5a7924e621e84c9280a9a27e1bcb7f6", 5)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest1.GetProto(),
				Data:   []byte("Hello"),
			},
			{
				Digest: digest2.GetProto(),
				Data:   []byte("World"),
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	chunkStorage.EXPECT().Put(ctx, digest1, gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), []byte("Hello"))
	})).Return(nil)
	chunkStorage.EXPECT().Put(ctx, digest2, gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), []byte("World"))
	})).Return(status.Error(codes.Internal, "Hard disk has a case of the Mondays"))

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000)

	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
		Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
			{
				Digest: digest1.GetProto(),
			},
			{
				Digest: digest2.GetProto(),
				Status: &status_pb.Status{
					Code:    int32(codes.Internal),
					Message: "Failed to save chunk: Hard disk has a case of the Mondays",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchUpdateBlobsMultiChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest1.GetProto(),
				Data:   []byte("Hello"),
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The blob is large enough relative to the minimum chunk size
	// that it cannot be stored as a single chunk, causing it to be
	// delegated to the ReaderPutter.
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 2, HorizonSizeBytes: 4}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(params, nil)
	readerPutter.EXPECT().PutReaderAt(gomock.Any(), digest1, gomock.Any(), params).DoAndReturn(
		func(ctx context.Context, d digest.Digest, r io.ReaderAt, params *remoteexecution.RepMaxCdcParams) error {
			data, err := io.ReadAll(io.NewSectionReader(r, 0, d.GetSizeBytes()))
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return status.Error(codes.Internal, "Storage backend on fire")
		},
	)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000)

	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
		Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
			{
				Digest: digest1.GetProto(),
				Status: &status_pb.Status{
					Code:    int32(codes.Internal),
					Message: "Storage backend on fire",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchUpdateBlobsZSTD(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	data := []byte("Hello")
	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	var compressed bytes.Buffer
	encoder, err := zstdPool.NewEncoder(ctx, &compressed)
	require.NoError(t, err)
	_, err = encoder.Write(data)
	require.NoError(t, err)
	require.NoError(t, encoder.Close())

	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(params, nil)
	chunkStorage.EXPECT().Put(ctx, digest1, gomock.Any()).Return(nil)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName: "ubuntu1804",
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest:     digest1.GetProto(),
				Data:       compressed.Bytes(),
				Compressor: remoteexecution.Compressor_ZSTD,
			},
		},
	}

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000)
	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.Responses, 1)
	require.Equal(t, codes.OK, codes.Code(response.Responses[0].Status.GetCode()))
}

func TestContentAddressableStorageServerBatchUpdateBlobsCorruptZSTD(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	// Decompression must fail before any interaction with the
	// Content Addressable Storage takes place.
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName: "ubuntu1804",
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest:     digest1.GetProto(),
				Data:       []byte("This is not a ZSTD stream"),
				Compressor: remoteexecution.Compressor_ZSTD,
			},
		},
	}

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000)
	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.Responses, 1)
	require.NotEqual(t, codes.OK, codes.Code(response.Responses[0].Status.GetCode()))
}

// allowAllAuthorizer authorizes all requests, so that authorization
// does not interfere with the behaviors under test.
var allowAllAuthorizer = auth.NewStaticAuthorizer(func(digest.InstanceName) bool { return true })

func TestContentAddressableStorageServerFindMissingBlobs(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 16)
	digest2 := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 256)

	request := &remoteexecution.FindMissingBlobsRequest{
		InstanceName: "my_instance_name",
		BlobDigests: []*remoteexecution.Digest{
			{Hash: digest1.GetHashString(), SizeBytes: digest1.GetSizeBytes()},
			{Hash: digest2.GetHashString(), SizeBytes: digest2.GetSizeBytes()},
		},
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	digests := digest.NewSetBuilder(2)
	digests.Add(digest1)
	digests.Add(digest2)

	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("my_instance_name")).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digests.Build()).Return(digest.EmptySet, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)

	maximumMessageSizeBytes := 200
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000)

	response, err := contentAddressableStorageServer.FindMissingBlobs(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetMissingBlobDigests())
}

func TestContentAddressableStorageServerFindMissingBlobsEmptyBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	emptyDigest := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	request := &remoteexecution.FindMissingBlobsRequest{
		InstanceName: "my_instance_name",
		BlobDigests:  []*remoteexecution.Digest{emptyDigest.GetProto()},
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The empty blob is always considered present. It may never be
	// reported as missing. The digest is filtered out before any
	// storage is consulted.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000)

	response, err := contentAddressableStorageServer.FindMissingBlobs(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetMissingBlobDigests())
}

func TestContentAddressableStorageServerBatchReadBlobsEmptyBlob(t *testing.T) {
	emptyDigest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)

	for _, tc := range []struct {
		name       string
		compressor remoteexecution.Compressor_Value
	}{
		{name: "IDENTITY", compressor: remoteexecution.Compressor_IDENTITY},
		{name: "ZSTD", compressor: remoteexecution.Compressor_ZSTD},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)

			request := &remoteexecution.BatchReadBlobsRequest{
				Digests:               []*remoteexecution.Digest{emptyDigest.GetProto()},
				InstanceName:          "ubuntu1804",
				AcceptableCompressors: []remoteexecution.Compressor_Value{tc.compressor},
			}

			chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
			chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
			cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
			zstdPool := zstd.NewPoolFromConfiguration(nil)

			// The empty blob is always present. Reading it yields
			// empty data, possibly compressed, without contacting
			// storage.
			cdcParametersFetcher.EXPECT().FetchCDCParameters(
				gomock.Any(),
				mustNewInstanceName("ubuntu1804"),
			).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

			readerPutter := mock.NewMockReaderPutter(ctrl)
			contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000)

			response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
			require.NoError(t, err)
			require.Len(t, response.Responses, 1)
			require.Equal(t, emptyDigest.GetProto(), response.Responses[0].Digest)
			require.NoError(t, status.ErrorProto(response.Responses[0].Status))
			require.Equal(t, tc.compressor, response.Responses[0].Compressor)
			if tc.compressor == remoteexecution.Compressor_IDENTITY {
				require.Empty(t, response.Responses[0].Data)
			} else {
				// The response contains a compressed stream that
				// decodes to no data at all.
				require.NotEmpty(t, response.Responses[0].Data)
				decoder, err := zstdPool.NewDecoder(ctx, bytes.NewReader(response.Responses[0].Data))
				require.NoError(t, err)
				decompressed, err := io.ReadAll(decoder)
				require.NoError(t, err)
				decoder.Close()
				require.Empty(t, decompressed)
			}
		})
	}
}

func TestContentAddressableStorageServerBatchUpdateBlobsEmptyBlob(t *testing.T) {
	emptyDigest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)

	for _, tc := range []struct {
		name       string
		compressor remoteexecution.Compressor_Value
	}{
		{name: "IDENTITY", compressor: remoteexecution.Compressor_IDENTITY},
		{name: "ZSTD", compressor: remoteexecution.Compressor_ZSTD},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)

			chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
			chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
			cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
			readerPutter := mock.NewMockReaderPutter(ctrl)
			zstdPool := zstd.NewPoolFromConfiguration(nil)

			// The empty blob decomposes into no chunks at all.
			// Uploading it succeeds without storing anything.
			cdcParametersFetcher.EXPECT().FetchCDCParameters(
				gomock.Any(),
				mustNewInstanceName("ubuntu1804"),
			).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

			var data []byte
			if tc.compressor == remoteexecution.Compressor_ZSTD {
				var compressed bytes.Buffer
				encoder, err := zstdPool.NewEncoder(ctx, &compressed)
				require.NoError(t, err)
				require.NoError(t, encoder.Close())
				data = compressed.Bytes()
			}

			request := &remoteexecution.BatchUpdateBlobsRequest{
				Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
					{
						Digest:     emptyDigest.GetProto(),
						Data:       data,
						Compressor: tc.compressor,
					},
				},
				InstanceName: "ubuntu1804",
			}

			contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000)

			response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
				Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
					{
						Digest: emptyDigest.GetProto(),
					},
				},
			}, response)
		})
	}
}

func TestContentAddressableStorageServerSplitBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunk1Digest, err := digestFunction.NewDigestFromProto(&remoteexecution.Digest{
		Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
		SizeBytes: 8,
	})
	require.NoError(t, err)
	chunk2Digest, err := digestFunction.NewDigestFromProto(&remoteexecution.Digest{
		Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
		SizeBytes: 8,
	})
	require.NoError(t, err)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 4, HorizonSizeBytes: 8}, nil)
	storedChunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, 16, false)
	require.NoError(t, err)
	chunkMappingStorage.EXPECT().Get(ctx, blobDigest).Return(
		storedChunkMapping,
		nil,
	)

	maximumMessageSizeBytes := 200
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000)

	response, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.ChunkDigests, 2)
	require.Equal(t, remoteexecution.ChunkingFunction_REP_MAX_CDC, response.ChunkingFunction)
}

func TestContentAddressableStorageServerSplitBlobSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The blob is small enough to fit in a single chunk. Such blobs
	// have no chunk mapping in storage, so splitting them is refused.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000)

	_, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings"), err)
}

func TestContentAddressableStorageServerSplitBlobNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// A single-chunk blob that does not exist. The trivial split is
	// refused before any storage interaction.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000)

	_, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings"), err)
}

func TestContentAddressableStorageServerSpliceBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SpliceBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		ChunkDigests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 8,
			},
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 8,
			},
		},
		InstanceName: "my_instance_name",
	}

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunk1Digest, err := digestFunction.NewDigestFromProto(request.ChunkDigests[0])
	require.NoError(t, err)
	chunk2Digest, err := digestFunction.NewDigestFromProto(request.ChunkDigests[1])
	require.NoError(t, err)

	expectedChunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, 16, false)
	require.NoError(t, err)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	chunkMappingStorage.EXPECT().Put(ctx, blobDigest, expectedChunkMapping).Return(nil)

	maximumMessageSizeBytes := 200

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000)
	response, err := contentAddressableStorageServer.SpliceBlob(ctx, request)
	require.NoError(t, err)
	require.Equal(t, request.BlobDigest, response.BlobDigest)
}

func TestContentAddressableStorageServerRegisterChunkMappingSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// A chunk mapping with fewer than two chunks is trivial. Blobs
	// that fit within a single chunk have no chunk mapping in storage,
	// so such registrations are rejected outright.
	blobDigest := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "e1bb3ab0d9402ae576a4f07d81f314f90a3e70672f5da6c21647e8ab507e511b", 18)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	readerPutter := mock.NewMockReaderPutter(ctrl)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000)

	for _, chunkDigests := range [][]*remoteexecution.Digest{
		{},
		{blobDigest.GetProto()},
	} {
		_, err := contentAddressableStorageServer.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
			InstanceName: "my_instance_name",
			BlobDigest:   blobDigest.GetProto(),
			ChunkDigests: chunkDigests,
		})
		require.Error(t, err)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	}
}

func TestContentAddressableStorageServerRegisterChunkMappingSizeOverflow(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// The sizes of the chunks in a chunk mapping must add up to the size
	// of the blob. As sizes are 64 bit values, adding them up may
	// overflow. Three chunks of size 2^63 - 1 and one of size 18 add up
	// to a total size of 2^64 + 16 bytes, which wraps around to 16. The
	// server should reject such lists instead of treating them as
	// composing into a blob of 16 bytes.
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	chunkDigests := make([]*remoteexecution.Digest, 4)
	for i := 0; i < 3; i++ {
		chunkDigests[i] = &remoteexecution.Digest{
			Hash:      fmt.Sprintf("%064d", i),
			SizeBytes: math.MaxInt64,
		}
	}
	chunkDigests[3] = &remoteexecution.Digest{
		Hash:      "0000000000000000000000000000000000000000000000000000000000000012",
		SizeBytes: 18,
	}

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, zstdPool, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000)
	_, err := contentAddressableStorageServer.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
		InstanceName: "my_instance_name",
		BlobDigest: &remoteexecution.Digest{
			Hash:      "0000000000000000000000000000000000000000000000000000000000000010",
			SizeBytes: 16,
		},
		ChunkDigests: chunkDigests,
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
