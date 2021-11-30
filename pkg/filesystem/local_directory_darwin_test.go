//go:build darwin
// +build darwin

package filesystem_test

import (
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestLocalDirectoryClonefileSuccess(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("source"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Clonefile(path.MustNewComponent("source"), d, path.MustNewComponent("target")))
	rf, err := d.OpenRead(path.MustNewComponent("target"))
	require.NoError(t, err)
	require.NoError(t, rf.Close())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryClonefileNotFound(t *testing.T) {
	d := openTmpDir(t)
	require.Equal(t, syscall.ENOENT, d.Clonefile(path.MustNewComponent("source"), d, path.MustNewComponent("target")))
	require.NoError(t, d.Close())
}
