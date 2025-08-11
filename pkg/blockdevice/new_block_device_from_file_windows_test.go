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
		path, err := blockdevice.RootPathForPath(`C:\`)
		require.NoError(t, err)
		require.Equal(t, `C:\`, path)

		path, err = blockdevice.RootPathForPath(`C:\somefolder`)
		require.NoError(t, err)
		require.Equal(t, `C:\`, path)

		path, err = blockdevice.RootPathForPath(`C:\somefolder\somefile.txt`)
		require.NoError(t, err)
		require.Equal(t, `C:\`, path)

		path, err = blockdevice.RootPathForPath(`C:/somefolder/somefile.txt`)
		require.NoError(t, err)
		require.Equal(t, `C:\`, path)
	})

	t.Run("UNCPath", func(t *testing.T) {
		// Test UNC paths (\\server\share)
		path, err := blockdevice.RootPathForPath(`\\server\share\`)
		require.NoError(t, err)
		require.Equal(t, `\\server\share\`, path)

		path, err = blockdevice.RootPathForPath(`\\server\share\folder\file.txt`)
		require.NoError(t, err)
		require.Equal(t, `\\server\share\`, path)

		path, err = blockdevice.RootPathForPath(`\\server\share/file.txt`)
		require.NoError(t, err)
		require.Equal(t, `\\server\share\`, path)

		path, err = blockdevice.RootPathForPath(`//server/share/file.txt`)
		require.NoError(t, err)
		require.Equal(t, `\\server\share\`, path)
	})

	t.Run("UnknownPath", func(t *testing.T) {
		// Test unhandled paths
		_, err := blockdevice.RootPathForPath("/uhoh/share")
		require.Error(t, err)
	})
}
