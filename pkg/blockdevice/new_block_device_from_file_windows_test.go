//go:build windows
// +build windows

package blockdevice_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/stretchr/testify/require"
)

func TestRootPathForPath(t *testing.T) {
	t.Run("ConventionalDiskPath", func(t *testing.T) {
		require.Equal(t, `C:\`, blockdevice.RootPathForPath(`C:\`))
		require.Equal(t, `C:\`, blockdevice.RootPathForPath(`C:\somefolder`))
		require.Equal(t, `C:\`, blockdevice.RootPathForPath(`C:\somefolder\somefile.txt`))
	})

	t.Run("UNCPath", func(t *testing.T) {
		// Test UNC paths (\\server\share)
		require.Equal(t, `\\server\share\`, blockdevice.RootPathForPath(`\\server\share\`))
		require.Equal(t, `\\server\share\`, blockdevice.RootPathForPath(`\\server\share\folder`))
		require.Equal(t, `\\server\share\`, blockdevice.RootPathForPath(`\\server\share\folder\file.txt`))
	})

	t.Run("UnknownPath", func(t *testing.T) {
		// Test unhandled paths
		require.Equal(t, "/uhoh/share", blockdevice.RootPathForPath("/uhoh/share"))
	})
}
