package configuration

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"

	"go.uber.org/mock/gomock"
)

func TestReadFallbackFindMissingReplicationConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		option                 string
		replicateOnFindMissing bool
	}{
		{name: "Omitted", replicateOnFindMissing: true},
		{name: "False", option: `, "disableFindMissingReplication": false`, replicateOnFindMissing: true},
		{name: "True", option: `, "disableFindMissingReplication": true`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)
			primary := mock.NewMockBlobAccess(ctrl)
			secondary := mock.NewMockBlobAccess(ctrl)
			creator := simpleNestedBlobAccessCreator{
				labels: map[string]BlobAccessInfo{
					"primary":   {BlobAccess: primary, DigestKeyFormat: digest.KeyWithoutInstance},
					"secondary": {BlobAccess: secondary, DigestKeyFormat: digest.KeyWithoutInstance},
				},
			}
			var configuration pb.BlobAccessConfiguration
			require.NoError(t, protojson.Unmarshal([]byte(`{
				"readFallback": {
					"primary": {"label": "primary"},
					"secondary": {"label": "secondary"},
					"replicator": {"local": {}}
					`+tc.option+`
				}
			}`), &configuration))
			info, err := creator.NewNestedBlobAccess(&configuration, NewCASBlobAccessCreator(nil, 1024, nil))
			require.NoError(t, err)

			helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
			digests := helloDigest.ToSingletonSet()
			primary.EXPECT().FindMissing(ctx, digests).Return(digests, nil)
			secondary.EXPECT().FindMissing(ctx, digests).Return(digest.EmptySet, nil)
			if tc.replicateOnFindMissing {
				secondary.EXPECT().Get(gomock.Any(), helloDigest).
					Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
				primary.EXPECT().Put(gomock.Any(), helloDigest, gomock.Any()).DoAndReturn(
					func(_ context.Context, _ digest.Digest, b buffer.Buffer) error {
						data, err := b.ToByteSlice(100)
						require.NoError(t, err)
						require.Equal(t, []byte("Hello"), data)
						return nil
					},
				)
			}

			missing, err := info.BlobAccess.FindMissing(ctx, digests)
			require.NoError(t, err)
			require.Equal(t, digest.EmptySet, missing)
		})
	}
}
