//go:build windows

package filesystem

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"

	"golang.org/x/sys/windows"
)

func TestLocalDirectoryRemovalDoesNotFollowWindowsLinks(t *testing.T) {
	for _, kind := range []string{"junction", "symlink"} {
		for _, operation := range []string{"RemoveAll", "RemoveAllChildren"} {
			t.Run(kind+"/"+operation, func(t *testing.T) {
				root, outside := t.TempDir(), t.TempDir()
				d := openRemovalDirectory(t, root)
				marker := filepath.Join(outside, "marker")
				require.NoError(t, os.WriteFile(marker, []byte("untouched"), 0o600))
				link := filepath.Join(root, "link")
				if kind == "junction" {
					createRemovalJunction(t, link, outside)
				} else {
					require.NoError(t, os.Symlink(outside, link))
				}
				if operation == "RemoveAll" {
					require.NoError(t, d.RemoveAll(path.MustNewComponent("link")))
				} else {
					require.NoError(t, d.RemoveAllChildren())
				}
				_, err := os.Lstat(link)
				require.True(t, os.IsNotExist(err))
				data, err := os.ReadFile(marker)
				require.NoError(t, err)
				require.Equal(t, "untouched", string(data))
			})
		}
	}
}

func TestLocalDirectoryRemovalKeepsWindowsDirectoryIdentity(t *testing.T) {
	parent, outside := t.TempDir(), t.TempDir()
	root, moved := filepath.Join(parent, "original"), filepath.Join(parent, "moved")
	require.NoError(t, os.Mkdir(root, 0o700))
	d := openRemovalDirectory(t, root)
	require.NoError(t, os.WriteFile(filepath.Join(root, "owned"), nil, 0o600))
	marker := filepath.Join(outside, "marker")
	require.NoError(t, os.WriteFile(marker, []byte("untouched"), 0o600))
	require.NoError(t, os.Rename(root, moved))
	createRemovalJunction(t, root, outside)
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "owned", entries[0].Name().String())
	require.NoError(t, d.RemoveAllChildren())
	remaining, err := os.ReadDir(moved)
	require.NoError(t, err)
	require.Empty(t, remaining)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
}

func TestLocalDirectoryRemovalChangedWindowsChildren(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	d := openRemovalDirectory(t, root)
	for _, name := range []string{"vanished", "replaced", "surviving"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o700))
	}
	// A missing entry must not stop cleanup of later siblings. A junction
	// replacing an enumerated directory must not redirect that cleanup.
	names := []string{"vanished", "replaced", "surviving"}
	for _, name := range names[:2] {
		require.NoError(t, os.Remove(filepath.Join(root, name)))
	}
	marker := filepath.Join(outside, "marker")
	require.NoError(t, os.WriteFile(marker, []byte("untouched"), 0o600))
	createRemovalJunction(t, filepath.Join(root, "replaced"), outside)
	require.NoError(t, os.WriteFile(filepath.Join(root, "surviving", "file"), nil, 0o600))
	require.NoError(t, d.removeChildren(nil, names))
	remaining, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, remaining)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
}

func TestLocalDirectoryRemoveAllMissingWindows(t *testing.T) {
	d := openRemovalDirectory(t, t.TempDir())
	require.NoError(t, d.RemoveAll(path.MustNewComponent("missing")))
}

func TestLocalDirectoryWindowsClosedHandle(t *testing.T) {
	for _, operation := range []string{"ReadDir", "RemoveAllChildren", "RemoveAll", "Sync"} {
		t.Run(operation, func(t *testing.T) {
			d, err := NewLocalDirectory(path.LocalFormat.NewParser(t.TempDir()))
			require.NoError(t, err)
			require.NoError(t, d.Close())
			switch operation {
			case "ReadDir":
				_, err = d.ReadDir()
			case "RemoveAllChildren":
				err = d.RemoveAllChildren()
			case "RemoveAll":
				err = d.RemoveAll(path.MustNewComponent("missing"))
			case "Sync":
				err = d.Sync()
			}
			require.ErrorIs(t, err, windows.ERROR_INVALID_HANDLE)
		})
	}
}

func TestLocalDirectoryWindowsReopenPermissionDenied(t *testing.T) {
	root := t.TempDir()
	d := openRemovalDirectory(t, root)
	original, err := windows.GetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
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
		require.NoError(t, windows.SetNamedSecurityInfo(root, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|protection, nil, nil, acl, nil))
	})
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	require.NoError(t, err)
	sid := user.User.Sid.String()
	// The retained handle does not grant the new FILE_LIST_DIRECTORY access.
	security, err := windows.SecurityDescriptorFromString("D:P(D;;0x00000001;;;" + sid + ")(A;;FA;;;" + sid + ")")
	require.NoError(t, err)
	denied, _, err := security.DACL()
	require.NoError(t, err)
	require.NoError(t, windows.SetNamedSecurityInfo(root, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, denied, nil))
	before, err := windows.GetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	_, err = d.ReadDir()
	require.True(t, os.IsPermission(err))
	require.True(t, os.IsPermission(d.RemoveAllChildren()))
	after, err := windows.GetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	require.Equal(t, before.String(), after.String())
}

func openRemovalDirectory(t *testing.T, root string) *localDirectory {
	t.Helper()
	directory, err := NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	return directory.(*localDirectory)
}

func createRemovalJunction(t *testing.T, link, target string) {
	t.Helper()
	require.NoError(t, os.Mkdir(link, 0o700))
	name, err := windows.UTF16PtrFromString(link)
	require.NoError(t, err)
	handle, err := windows.CreateFile(name, windows.GENERIC_WRITE, 0, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	require.NoError(t, err)
	defer windows.CloseHandle(handle)
	substitute, err := windows.UTF16FromString(`\??\` + target)
	require.NoError(t, err)
	printName, err := windows.UTF16FromString(target)
	require.NoError(t, err)
	names := append(substitute, printName...)
	buffer := make([]byte, 16+2*len(names))
	binary.LittleEndian.PutUint32(buffer[0:4], windows.IO_REPARSE_TAG_MOUNT_POINT)
	binary.LittleEndian.PutUint16(buffer[4:6], uint16(len(buffer)-8))
	binary.LittleEndian.PutUint16(buffer[10:12], uint16(2*(len(substitute)-1)))
	binary.LittleEndian.PutUint16(buffer[12:14], uint16(2*len(substitute)))
	binary.LittleEndian.PutUint16(buffer[14:16], uint16(2*(len(printName)-1)))
	for i, character := range names {
		binary.LittleEndian.PutUint16(buffer[16+2*i:18+2*i], character)
	}
	var returned uint32
	require.NoError(t, windows.DeviceIoControl(handle, windows.FSCTL_SET_REPARSE_POINT,
		&buffer[0], uint32(len(buffer)), nil, 0, &returned, nil))
}
