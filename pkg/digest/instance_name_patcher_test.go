package digest_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
)

func testPatcher(t *testing.T, ip digest.InstanceNamePatcher, oldInstanceName, newInstanceName string) {
	require.Equal(
		t,
		util.Must(digest.NewInstanceName(newInstanceName)),
		ip.PatchInstanceName(util.Must(digest.NewInstanceName(oldInstanceName))))
	require.Equal(
		t,
		digest.MustNewDigest(newInstanceName, remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		ip.PatchDigest(digest.MustNewDigest(oldInstanceName, remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)))
	require.Equal(
		t,
		digest.MustNewDigest(oldInstanceName, remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		ip.UnpatchDigest(digest.MustNewDigest(newInstanceName, remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)))
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
			util.Must(digest.NewInstanceName("a/b/c")),
			util.Must(digest.NewInstanceName("a/b/c")),
		)

		testPatcher(t, ip, "a/b/c", "a/b/c")
		testPatcher(t, ip, "a/b/c/x", "a/b/c/x")
	})

	t.Run("GrowEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			digest.EmptyInstanceName,
			util.Must(digest.NewInstanceName("a")),
		)

		testPatcher(t, ip, "", "a")
		testPatcher(t, ip, "x", "a/x")
	})

	t.Run("GrowNonEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			util.Must(digest.NewInstanceName("a")),
			util.Must(digest.NewInstanceName("a/b")),
		)

		testPatcher(t, ip, "a", "a/b")
		testPatcher(t, ip, "a/x", "a/b/x")
	})

	t.Run("ShrinkEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			util.Must(digest.NewInstanceName("a")),
			digest.EmptyInstanceName,
		)

		testPatcher(t, ip, "a", "")
		testPatcher(t, ip, "a/x", "x")
	})

	t.Run("ShrinkNonEmpty", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			util.Must(digest.NewInstanceName("a/b")),
			util.Must(digest.NewInstanceName("a")),
		)

		testPatcher(t, ip, "a/b", "a")
		testPatcher(t, ip, "a/b/x", "a/x")
	})

	t.Run("ChangePrefix", func(t *testing.T) {
		ip := digest.NewInstanceNamePatcher(
			util.Must(digest.NewInstanceName("a")),
			util.Must(digest.NewInstanceName("b")),
		)

		testPatcher(t, ip, "a", "b")
		testPatcher(t, ip, "a/x", "b/x")
	})
}
