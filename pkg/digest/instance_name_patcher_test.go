package digest_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func testPatcher(t *testing.T, ip digest.InstanceNamePatcher, oldInstanceName string, newInstanceName string) {
	require.Equal(
		t,
		digest.MustNewInstanceName(newInstanceName),
		ip.PatchInstanceName(digest.MustNewInstanceName(oldInstanceName)))
	require.Equal(
		t,
		digest.MustNewDigest(newInstanceName, "8b1a9953c4611296a827abf8c47804d7", 5),
		ip.PatchDigest(digest.MustNewDigest(oldInstanceName, "8b1a9953c4611296a827abf8c47804d7", 5)))
	require.Equal(
		t,
		digest.MustNewDigest(oldInstanceName, "8b1a9953c4611296a827abf8c47804d7", 5),
		ip.UnpatchDigest(digest.MustNewDigest(newInstanceName, "8b1a9953c4611296a827abf8c47804d7", 5)))
}

func TestInstanceNamePatcher(t *testing.T) {
	t.Run("IdentityEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.EmptyInstanceName,
			digest.EmptyInstanceName,
		)

		testPatcher(t, ip, "", "")
		testPatcher(t, ip, "x", "x")
	})

	t.Run("IdentityNonEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.MustNewInstanceName("a/b/c"),
			digest.MustNewInstanceName("a/b/c"),
		)

		testPatcher(t, ip, "a/b/c", "a/b/c")
		testPatcher(t, ip, "a/b/c/x", "a/b/c/x")
	})

	t.Run("GrowEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.EmptyInstanceName,
			digest.MustNewInstanceName("a"),
		)

		testPatcher(t, ip, "", "a")
		testPatcher(t, ip, "x", "a/x")
	})

	t.Run("GrowNonEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.MustNewInstanceName("a"),
			digest.MustNewInstanceName("a/b"),
		)

		testPatcher(t, ip, "a", "a/b")
		testPatcher(t, ip, "a/x", "a/b/x")
	})

	t.Run("ShrinkEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.MustNewInstanceName("a"),
			digest.EmptyInstanceName,
		)

		testPatcher(t, ip, "a", "")
		testPatcher(t, ip, "a/x", "x")
	})

	t.Run("ShrinkNonEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.MustNewInstanceName("a/b"),
			digest.MustNewInstanceName("a"),
		)

		testPatcher(t, ip, "a/b", "a")
		testPatcher(t, ip, "a/b/x", "a/x")
	})

	t.Run("ChangePrefix", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.MustNewInstanceName("a"),
			digest.MustNewInstanceName("b"),
		)

		testPatcher(t, ip, "a", "b")
		testPatcher(t, ip, "a/x", "b/x")
	})
}
