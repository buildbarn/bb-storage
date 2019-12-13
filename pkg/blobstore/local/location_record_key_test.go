package local_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestLocationRecordKey(t *testing.T) {
	key := local.NewLocationRecordKey(
		util.MustNewDigest(
			"ignored",
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 123,
			}))
	key.Attempt = 0x11223344

	// FNV-1a(0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85544332211) == 0x3a62c67919ee2436.
	// This implementation folds the high bits into the low ones,
	// which is why only the top 32 bits equal FNV-1a.
	require.Equal(t, uint64(0x3a62c679238ce24f), key.Hash(14695981039346656037))
}
