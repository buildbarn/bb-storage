package cas_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestEmptyBlobReads validates that the empty blob is implicitly
// present on every read path of the cas package: reading a zero-sized
// digest must yield empty data without error. By definition in the
// Remote Execution API, a zero-sized blob is always present, while none
// of the write paths ever store it. Reads must therefore not depend on
// the Chunk Storage (CS) or Chunk Mapping Storage (CMS) containing
// anything for the empty blob.
//
// The Chunk Storage and Chunk Mapping Storage mocks are created without
// any expectations, so any call that is attributed to the empty blob
// causes the test to fail. Calls that are independent of the empty
// blob, such as the chunk storages being asked to find missing blobs,
// are permitted explicitly.
func TestEmptyBlobReads(t *testing.T) {
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}
	emptyDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)

	for _, tc := range []struct {
		name string
		run  func(t *testing.T, deps emptyBlobTestDeps)
	}{
		{
			name: "GetBytes",
			run: func(t *testing.T, deps emptyBlobTestDeps) {
				data, err := cas.GetBytes(deps.ctx, deps.chunkBytesReader, nil, params, emptyDigest, 1024)
				require.NoError(t, err)
				require.Empty(t, data)
			},
		},
		{
			name: "GetReader",
			run: func(t *testing.T, deps emptyBlobTestDeps) {
				r, err := cas.GetReader(deps.ctx, deps.chunkBytesReader, nil, params, emptyDigest, 0)
				require.NoError(t, err)
				data, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Empty(t, data)
			},
		},
		{
			name: "IntoWriter",
			run: func(t *testing.T, deps emptyBlobTestDeps) {
				var w bytes.Buffer
				require.NoError(t, cas.IntoWriter(deps.ctx, deps.chunkBytesReader, nil, params, emptyDigest, 0, &w))
				require.Empty(t, w.Bytes())
			},
		},
		{
			name: "ReadBlobAt",
			run: func(t *testing.T, deps emptyBlobTestDeps) {
				n, err := cas.ReadBlobAt(deps.ctx, deps.chunkBytesReader, nil, params, emptyDigest, make([]byte, 16), 0)
				require.Equal(t, 0, n)
				require.Equal(t, io.EOF, err)
			},
		},
		{
			name: "ReadStream",
			run: func(t *testing.T, deps emptyBlobTestDeps) {
				r, err := deps.streamReader.ReadStream(deps.ctx, emptyDigest)
				require.NoError(t, err)
				data, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Empty(t, data)
			},
		},
		{
			name: "FindMissing",
			run: func(t *testing.T, deps emptyBlobTestDeps) {
				deps.chunkStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
				deps.chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
				missing, err := cas.FindMissing(deps.ctx, deps.chunkStorage, deps.chunkMappingStorage, params, emptyDigest.ToSingletonSet())
				require.NoError(t, err)
				require.True(t, missing.Empty())
			},
		},
		{
			name: "MessageReader",
			run: func(t *testing.T, deps emptyBlobTestDeps) {
				deps.cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), gomock.Any()).Return(params, nil).AnyTimes()
				msg, err := cas.NewMessageReader[remoteexecution.Directory](deps.chunkBytesReader, nil, deps.cdcParametersFetcher, 1024).Read(deps.ctx, emptyDigest)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.Directory{}, msg)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)
			chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
			cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
			tc.run(t, emptyBlobTestDeps{
				ctx:                  ctx,
				chunkBytesReader:     cas.NewChunkBytesReader(chunkStorage),
				streamReader:         cas.NewStorageBackedStreamReader(cas.NewChunkBytesReader(chunkStorage), nil, cdcParametersFetcher),
				chunkStorage:         chunkStorage,
				chunkMappingStorage:  mock.NewMockBlobAccess[chunk.Mapping](ctrl),
				cdcParametersFetcher: cdcParametersFetcher,
			})
		})
	}
}

type emptyBlobTestDeps struct {
	ctx                  context.Context
	chunkBytesReader     reader.Reader[[]byte]
	streamReader         cas.StreamReader
	chunkStorage         *mock.MockBlobAccess[*chunk.Chunk]
	chunkMappingStorage  *mock.MockBlobAccess[chunk.Mapping]
	cdcParametersFetcher *mock.MockCDCParametersFetcher
}
