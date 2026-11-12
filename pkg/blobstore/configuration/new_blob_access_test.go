package configuration_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	lossymap_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/lossymap"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLocalBlobAccessRefreshConcurrency(t *testing.T) {
	for _, tc := range []struct {
		name         string
		concurrency  uint32
		hierarchical bool
		wantErr      error
	}{
		{
			name:    "Unset",
			wantErr: status.Error(codes.InvalidArgument, "Refresh concurrency must be between 1 and 256, got 0"),
		},
		{name: "Single", concurrency: 1},
		{name: "Maximum", concurrency: 256},
		{
			name:        "AboveMaximum",
			concurrency: 257,
			wantErr:     status.Error(codes.InvalidArgument, "Refresh concurrency must be between 1 and 256, got 257"),
		},
		{
			name:        "MaximumUint32",
			concurrency: ^uint32(0),
			wantErr:     status.Error(codes.InvalidArgument, "Refresh concurrency must be between 1 and 256, got 4294967295"),
		},
		{name: "HierarchicalUnset", hierarchical: true},
		{
			name:         "HierarchicalExplicit",
			concurrency:  1,
			hierarchical: true,
			wantErr:      status.Error(codes.InvalidArgument, "Refresh concurrency is not supported with hierarchical instance names"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := &pb.LocalBlobAccessConfiguration{
				RefreshConcurrency:        tc.concurrency,
				HierarchicalInstanceNames: tc.hierarchical,
			}
			if tc.wantErr == nil {
				local.BlocksBackend = &pb.LocalBlobAccessConfiguration_BlocksInMemory_{
					BlocksInMemory: &pb.LocalBlobAccessConfiguration_BlocksInMemory{BlockSizeBytes: 4096},
				}
				local.KeyLocationMap = &lossymap_pb.HashMapConfiguration{
					Backend: &lossymap_pb.HashMapConfiguration_InMemory_{
						InMemory: &lossymap_pb.HashMapConfiguration_InMemory{Entries: 17},
					},
					MaximumGetAttempts: 16,
					MaximumPutAttempts: 64,
				}
				local.OldBlocks = 1
				local.CurrentBlocks = 1
				local.NewBlocks = 1
			}
			// Invalid limits must fail before constructing storage, even
			// when no backing store is configured.
			info, err := configuration.NewBlobAccessFromConfiguration(nil, &pb.BlobAccessConfiguration{
				Backend: &pb.BlobAccessConfiguration_Local{Local: local},
			}, configuration.NewCSBlobAccessCreator(nil, 4096))
			testutil.RequireEqualStatus(t, tc.wantErr, err)
			if tc.wantErr == nil {
				require.NotNil(t, info.BlobAccess)
			} else {
				require.Nil(t, info.BlobAccess)
			}
		})
	}
}

func TestLocalBlobAccessConfigurationFieldNumbers(t *testing.T) {
	fields := (&pb.LocalBlobAccessConfiguration{}).ProtoReflect().Descriptor().Fields()
	require.Equal(t, 15, int(fields.ByName("chunking_parameters").Number()))
	require.Equal(t, 16, int(fields.ByName("refresh_concurrency").Number()))
}
