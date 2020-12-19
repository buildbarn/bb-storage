package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestComponent(t *testing.T) {
	t.Run("Invalid", func(t *testing.T) {
		_, ok := path.NewComponent("")
		require.False(t, ok)

		_, ok = path.NewComponent(".")
		require.False(t, ok)

		_, ok = path.NewComponent("..")
		require.False(t, ok)

		_, ok = path.NewComponent("foo/bar")
		require.False(t, ok)
	})

	t.Run("Valid", func(t *testing.T) {
		c, ok := path.NewComponent("hello")
		require.True(t, ok)
		require.Equal(t, "hello", c.String())
	})
}
