package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/stretchr/testify/require"
)

func TestInMemoryLocationRecordArray(t *testing.T) {
	array := local.NewInMemoryLocationRecordArray(1024)

	// Entries should be default initialized.
	record, err := array.Get(123)
	require.NoError(t, err)
	require.Equal(t, local.LocationRecord{}, record)

	// Entries should be writable.
	record1 := local.LocationRecord{
		Key: local.LocationRecordKey{
			Digest: local.NewCompactDigest("3e25960a79dbc69b674cd4ec67a72c62-123-hello"),
		},
		Location: local.Location{
			BlockID:     123,
			OffsetBytes: 456,
			SizeBytes:   789,
		},
	}
	err = array.Put(123, record1)
	require.NoError(t, err)
	record, err = array.Get(123)
	require.NoError(t, err)
	require.Equal(t, record1, record)

	// Entries should be overwritable.
	record2 := local.LocationRecord{
		Key: local.LocationRecordKey{
			Digest: local.NewCompactDigest("04da22ebda78f235062bea9c6786029a-456-hello"),
		},
		Location: local.Location{
			BlockID:     483,
			OffsetBytes: 32984729387,
			SizeBytes:   58974582,
		},
	}
	err = array.Put(123, record2)
	require.NoError(t, err)

	record, err = array.Get(123)
	require.NoError(t, err)
	require.Equal(t, record2, record)
}
