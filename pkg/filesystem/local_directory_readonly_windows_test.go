//go:build windows

package filesystem_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestLocalDirectoryRemoveReadOnly(t *testing.T) {
	for _, operation := range []string{"Remove", "RemoveAll", "RemoveAllChildren"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			cached := filepath.Join(t.TempDir(), "cached")
			require.NoError(t, os.WriteFile(cached, []byte("cached input"), 0o600))
			require.NoError(t, os.Chmod(cached, 0o400))
			t.Cleanup(func() { require.NoError(t, os.Chmod(cached, 0o600)) })
			before, err := windows.GetNamedSecurityInfo(cached, windows.SE_FILE_OBJECT,
				windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
			require.NoError(t, err)
			require.NoError(t, os.Link(cached, filepath.Join(root, "input")))
			d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })

			switch operation {
			case "Remove":
				require.NoError(t, d.Remove(path.MustNewComponent("input")))
			case "RemoveAll":
				require.NoError(t, d.RemoveAll(path.MustNewComponent("input")))
			case "RemoveAllChildren":
				// Also cover a read-only directory containing a read-only file.
				child := filepath.Join(root, "directory")
				require.NoError(t, os.Mkdir(child, 0o700))
				require.NoError(t, os.Link(cached, filepath.Join(child, "input")))
				require.NoError(t, os.Chmod(child, 0o500))
				require.NoError(t, d.RemoveAllChildren())
			}
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			require.Empty(t, entries)
			data, err := os.ReadFile(cached)
			require.NoError(t, err)
			require.Equal(t, "cached input", string(data))
			info, err := os.Stat(cached)
			require.NoError(t, err)
			require.Zero(t, info.Mode().Perm()&0o222)
			after, err := windows.GetNamedSecurityInfo(cached, windows.SE_FILE_OBJECT,
				windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
			require.NoError(t, err)
			require.Equal(t, before.String(), after.String())
		})
	}
}

func TestLocalDirectoryRemoveReadOnlyPermissionDenied(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(file, []byte("protected"), 0o600))
	require.NoError(t, os.Chmod(file, 0o400))
	t.Cleanup(func() { require.NoError(t, os.Chmod(file, 0o600)) })
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })

	// Windows permits deletion through either DELETE on the file or
	// FILE_DELETE_CHILD on its parent. Deny both, without changing ownership.
	denyReadOnlyRemovalAccess(t, file, "SD")
	denyReadOnlyRemovalAccess(t, root, "0x00000040")
	require.True(t, os.IsPermission(d.Remove(path.MustNewComponent("file"))))
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "protected", string(data))
}

func denyReadOnlyRemovalAccess(t *testing.T, name, rights string) {
	t.Helper()
	original, err := windows.GetNamedSecurityInfo(name, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	acl, _, err := original.DACL()
	require.NoError(t, err)
	control, _, err := original.Control()
	require.NoError(t, err)
	protection := windows.SECURITY_INFORMATION(windows.UNPROTECTED_DACL_SECURITY_INFORMATION)
	if control&windows.SE_DACL_PROTECTED != 0 {
		protection = windows.PROTECTED_DACL_SECURITY_INFORMATION
	}
	t.Cleanup(func() {
		require.NoError(t, windows.SetNamedSecurityInfo(name, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|protection, nil, nil, acl, nil))
	})
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	require.NoError(t, err)
	sid := user.User.Sid.String()
	security, err := windows.SecurityDescriptorFromString("D:P(D;;" + rights + ";;;" + sid + ")(A;;FA;;;" + sid + ")")
	require.NoError(t, err)
	denied, _, err := security.DACL()
	require.NoError(t, err)
	require.NoError(t, windows.SetNamedSecurityInfo(name, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, denied, nil))
}
