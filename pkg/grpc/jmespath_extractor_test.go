package grpc_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestJMESPathMetadataExtractorSimple(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{
		"hdr-from-auth": [authenticationMetadata.public],
		"hdr-from-incoming": incomingGRPCMetadata.whiz,
		"this-is-static": ['and great'],
		"hdr-from-both": [incomingGRPCMetadata.whiz[0], authenticationMetadata.public],
		"optional-hdr": incomingGRPCMetadata.missing
	}`), nil)
	require.NoError(t, err)

	// We compare with metadata.Pairs because JMESPath evaluation traverses maps
	// in arbitrary orders, and Pairs makes the comparisons order-independent.
	want := metadata.Pairs(grpc.MetadataHeaderValues([]string{
		"hdr-from-auth", "boop",
		"hdr-from-incoming", "bang",
		"this-is-static", "and great",
		"hdr-from-both", "bang",
		"hdr-from-both", "boop",
	})...)

	ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewStringValue("boop"),
	})))

	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("whiz", "bang"))

	pairsSlice, err := extractor(ctx)
	require.NoError(t, err)
	pairs := metadata.Pairs(pairsSlice...)

	require.Equal(t, want, pairs)
}

func TestJMESPathMetadataExtractorAuthMatchToString(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": authenticationMetadata.public}`), nil)
	require.NoError(t, err)

	// The resulting header value must be a list. Yielding a string
	// value directly should cause an error.
	ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewStringValue("boop"),
	})))

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-slice metadata value"), err)
}

func TestJMESPathMetadataExtractorAuthMatchToHeterogenousSlice(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": authenticationMetadata.public}`), nil)
	require.NoError(t, err)

	// Each of the header values should be a valid string. Integer
	// values are not permitted.
	ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewListValue(&structpb.ListValue{
			Values: []*structpb.Value{
				structpb.NewStringValue("boop"),
				structpb.NewNumberValue(1),
			},
		}),
	})))

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-string metadata value"), err)
}

func TestNewJMESPathMetadataFileProvider(t *testing.T) {
	// Create a temporary file with test content.
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test-token")
	err := os.WriteFile(filePath, []byte("token-value1"), 0o644)
	require.NoError(t, err)

	// Build the extractor.
	context, cancel := context.WithCancel(context.Background())
	provider, err := grpc.NewJMESPathMetadataFileProvider(context, []*pb.ClientConfiguration_RefreshedFile{
		{
			Key:             "token",
			Path:            filePath,
			RefreshInterval: durationpb.New(time.Millisecond),
		},
	})
	require.NoError(t, err)
	extractor, err := grpc.NewJMESPathMetadataExtractor(
		jmespath.MustCompile(`{"authorization": [files.token]}`),
		provider,
	)
	require.NoError(t, err)

	// Validate the initial contents are correct.
	headers, err := extractor(context)
	require.NoError(t, err)
	want := grpc.MetadataHeaderValues([]string{
		"authorization", "token-value1",
	})
	require.Equal(t, want, headers)

	// Modify the file.
	err = os.WriteFile(filePath, []byte("token-value2"), 0o644)
	require.NoError(t, err)

	// Wait for the file to be reloaded. This is potentially fragile.
	time.Sleep(time.Second)

	// Validate the updated contents are correct.
	headers, err = extractor(context)
	require.NoError(t, err)
	want = grpc.MetadataHeaderValues([]string{
		"authorization", "token-value2",
	})
	require.Equal(t, want, headers)

	// Cancel the context to stop the file reloading.
	cancel()
	time.Sleep(time.Second)
}
