package digest_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func TestSetBuilderEmpty(t *testing.T) {
	// For unit testing purposes, empty sets created through
	// SetBuilder must be deeply equal to EmptySet. This means that
	// the slice of digests stored within is nil.
	//
	// This also cuts down the number of memory allocations. It is
	// fairly common for BlobAccess.FindMissing() to return empty
	// sets. Letting those use an additional allocation would be
	// wasteful.
	require.Equal(t, digest.EmptySet, digest.NewSetBuilder().Build())
}
