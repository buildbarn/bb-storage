package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestJMESPathMetadataExtractorSimple(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{
        "hdr-from-auth": [authenticationMetadata.beep],
		"hdr-from-incoming": incomingGRPCMetadata.whiz,
		"this-is-static": ['and great'],
		"hdr-from-both": [incomingGRPCMetadata.whiz[0], authenticationMetadata.beep],
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

	ctx := context.WithValue(context.Background(), auth.AuthenticationMetadata{}, map[string]interface{}{
		"beep": "boop",
	})

	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("whiz", "bang"))

	pairsSlice, err := extractor(ctx)
	require.NoError(t, err)
	pairs := metadata.Pairs(pairsSlice...)

	require.Equal(t, want, pairs)
}

func TestJMESPathMetadataExtractorAuthMetadataMapToString(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": [authenticationMetadata.beep]}`))
	require.NoError(t, err)

	// jmespath hard-codes map[string]interface{} as a valid type,
	// but ignores map[string]string
	// Make sure we give a good error in this case
	ctx := context.WithValue(context.Background(), auth.AuthenticationMetadata{}, map[string]string{"beep": "boop"})

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-string metadata value"), err)
}

func TestJMESPathMetadataExtractorAuthMetadataMapToStringSlice(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": [authenticationMetadata.beep]}`))
	require.NoError(t, err)

	// jmespath hard-codes map[string]interface{} as a valid type,
	// but ignores map[string]string
	// Make sure we give a good error in this case
	ctx := context.WithValue(context.Background(), auth.AuthenticationMetadata{}, map[string][]string{"beep": {"boop"}})

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-string metadata value"), err)
}

func TestJMESPathMetadataExtractorAuthMatchToString(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": authenticationMetadata.beep}`))
	require.NoError(t, err)

	// jmespath hard-codes map[string]interface{} as a valid type,
	// but ignores map[string]string
	// Make sure we give a good error in this case
	ctx := context.WithValue(context.Background(), auth.AuthenticationMetadata{}, map[string]interface{}{"beep": "boop"})

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-slice metadata value"), err)
}

func TestJMESPathMetadataExtractorAuthMatchToHeterogenousSlice(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": authenticationMetadata.beep}`))
	require.NoError(t, err)

	// jmespath hard-codes map[string]interface{} as a valid type,
	// but ignores map[string]string
	// Make sure we give a good error in this case
	ctx := context.WithValue(context.Background(), auth.AuthenticationMetadata{}, map[string]interface{}{"beep": []interface{}{"boop", 1}})

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-string metadata value"), err)
}
