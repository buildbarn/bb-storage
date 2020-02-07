package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func TestInMemoryLocationRecordArray(t *testing.T) {
	array := local.NewInMemoryLocationRecordArray(1024)

	// Entries should be default initialized.
	require.Equal(t, local.LocationRecord{}, array.Get(123))

	// Entries should be writable.
	record1 := local.LocationRecord{
		Key: local.NewLocationRecordKey(
			digest.MustNewDigest(
				"hello",
				"3e25960a79dbc69b674cd4ec67a72c62",
				123)),
		Location: local.Location{
			BlockID:     123,
			OffsetBytes: 456,
			SizeBytes:   789,
		},
	}
	array.Put(123, record1)
	require.Equal(t, record1, array.Get(123))

	// Entries should be overwritable.
	record2 := local.LocationRecord{
		Key: local.NewLocationRecordKey(
			digest.MustNewDigest(
				"foo",
				"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				123)),
		Location: local.Location{
			BlockID:     483,
			OffsetBytes: 32984729387,
			SizeBytes:   58974582,
		},
	}
	array.Put(123, record2)
	require.Equal(t, record2, array.Get(123))
}
