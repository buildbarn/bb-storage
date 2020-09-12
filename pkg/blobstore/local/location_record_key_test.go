package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/stretchr/testify/require"
)

func TestLocationRecordKey(t *testing.T) {
	recordKey := local.LocationRecordKey{
		Key: local.Key{
			0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
			0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
			0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
			0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
		},
		Attempt: 0x11223344,
	}

	// FNV-1a(0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85544332211) == 0x3a62c67919ee2436.
	// This implementation folds the high bits into the low ones,
	// which is why only the top 32 bits equal FNV-1a.
	require.Equal(t, uint64(0x3a62c679238ce24f), recordKey.Hash(14695981039346656037))
}
