package digest_test

import (
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestExistenceCache(t *testing.T) {
	ctrl := gomock.NewController(t)

	clock := mock.NewMockClock(ctrl)
	existenceCache := digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 2, time.Minute, eviction.NewLRUSet[string]())

	digests := []digest.Digest{
		digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 5),
		digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7),
		digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "ebbbb099e9d2f7892d97ab3640ae8283", 9),
	}
	allDigests := digest.NewSetBuilder().
		Add(digests[0]).
		Add(digests[1]).
		Add(digests[2]).
		Build()

	// RemoveExisting() should not remove any digests initially.
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	require.Equal(
		t,
		allDigests,
		existenceCache.RemoveExisting(allDigests))

	// Deleting nothing should have no effect.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	existenceCache.Add(digest.EmptySet)
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	require.Equal(
		t,
		allDigests,
		existenceCache.RemoveExisting(allDigests))

	// Mark the first two elements as existing. RemoveExisting()
	// should now start pruning them from the input set.
	clock.EXPECT().Now().Return(time.Unix(1003, 0))
	existenceCache.Add(digest.NewSetBuilder().
		Add(digests[0]).
		Add(digests[1]).
		Build())
	clock.EXPECT().Now().Return(time.Unix(1004, 0))
	require.Equal(
		t,
		digests[2].ToSingletonSet(),
		existenceCache.RemoveExisting(allDigests))

	// If we touch digests[1] and insert digests[2], digests[0]
	// should be knocked out of the LRU cache.
	clock.EXPECT().Now().Return(time.Unix(1005, 0))
	require.Equal(
		t,
		digest.EmptySet,
		existenceCache.RemoveExisting(digest.NewSetBuilder().
			Add(digests[1]).
			Build()))
	clock.EXPECT().Now().Return(time.Unix(1006, 0))
	existenceCache.Add(digest.NewSetBuilder().
		Add(digests[2]).
		Build())
	clock.EXPECT().Now().Return(time.Unix(1007, 0))
	require.Equal(
		t,
		digests[0].ToSingletonSet(),
		existenceCache.RemoveExisting(allDigests))

	// digests[1] was inserted at t = 1003, so it should disappear
	// after t = 1063.
	clock.EXPECT().Now().Return(time.Unix(1063, 0))
	require.Equal(
		t,
		digest.NewSetBuilder().
			Add(digests[0]).
			Build(),
		existenceCache.RemoveExisting(allDigests))
	clock.EXPECT().Now().Return(time.Unix(1063, 1))
	require.Equal(
		t,
		digest.NewSetBuilder().
			Add(digests[0]).
			Add(digests[1]).
			Build(),
		existenceCache.RemoveExisting(allDigests))

	// digests[2] was inserted at t = 1006, so it should disappear
	// after t = 1066.
	clock.EXPECT().Now().Return(time.Unix(1066, 0))
	require.Equal(
		t,
		digest.NewSetBuilder().
			Add(digests[0]).
			Add(digests[1]).
			Build(),
		existenceCache.RemoveExisting(allDigests))
	clock.EXPECT().Now().Return(time.Unix(1066, 1))
	require.Equal(
		t,
		allDigests,
		existenceCache.RemoveExisting(allDigests))
}
