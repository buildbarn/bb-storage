package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/jmespath/go-jmespath"
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

	ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewStringValue("boop"),
	}))

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
	ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewStringValue("boop"),
	}))

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-slice metadata value"), err)
}

func TestJMESPathMetadataExtractorAuthMatchToHeterogenousSlice(t *testing.T) {
	extractor, err := grpc.NewJMESPathMetadataExtractor(jmespath.MustCompile(`{"hdr": authenticationMetadata.public}`))
	require.NoError(t, err)

	// Each of the header values should be a valid string. Integer
	// values are not permitted.
	ctx := auth.NewContextWithAuthenticationMetadata(context.Background(), auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Public: structpb.NewListValue(&structpb.ListValue{
			Values: []*structpb.Value{
				structpb.NewStringValue("boop"),
				structpb.NewNumberValue(1),
			},
		}),
	}))

	_, err = extractor(ctx)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "Failed to extract JMESPath result: Non-string metadata value"), err)
}
