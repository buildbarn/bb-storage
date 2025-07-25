package grpc_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/jmespath"
	"github.com/buildbarn/bb-storage/pkg/program"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	jmespath_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/jmespath"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestJMESPathMetadataExtractorSimple(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{
		"hdr-from-auth": [authenticationMetadata.public],
		"hdr-from-incoming": incomingGRPCMetadata.whiz,
		"this-is-static": ['and great'],
		"hdr-from-both": [incomingGRPCMetadata.whiz[0], authenticationMetadata.public],
		"optional-hdr": incomingGRPCMetadata.missing
	}`))
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
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": authenticationMetadata.public}`))
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
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": authenticationMetadata.public}`))
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
	// Build the extractor.
	ctx, cancel := context.WithCancel(context.Background())
	program.RunLocal(ctx, func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		// Create a temporary file with test content.
		tempDir := t.TempDir()
		filePath := filepath.Join(tempDir, "test-token")
		err := os.WriteFile(filePath, []byte("token-value1"), 0o644)
		require.NoError(t, err)

		expr, err := jmespath.NewExpressionFromConfiguration(
			&jmespath_pb.Expression{
				Expression: `{"authorization": [files.token]}`,
				Files: []*jmespath_pb.File{
					{
						Key:  "token",
						Path: filePath,
					},
				},
				TestVectors: []*jmespath_pb.TestVector{
					{
						Input: util.Must(structpb.NewStruct(map[string]any{
							"files": map[string]any{
								"token": "tv-token-value",
							},
						})),
						ExpectedOutput: util.Must(structpb.NewValue(map[string]any{
							"authorization": []any{"tv-token-value"},
						})),
					},
				},
			},
			siblingsGroup,
			clock.SystemClock,
		)
		require.NoError(t, err)
		extractor, err := grpc.NewJMESPathMetadataExtractor(expr)
		require.NoError(t, err)

		headers, err := extractor(ctx)
		require.NoError(t, err)
		want := grpc.MetadataHeaderValues([]string{
			"authorization", "token-value1",
		})
		require.Equal(t, want, headers)

		// Cancel the context to stop the file reloading.
		cancel()
		return nil
	})
}
