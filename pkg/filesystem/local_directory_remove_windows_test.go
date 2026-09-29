//go:build windows

package filesystem_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
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

func TestLocalDirectoryRemoveAllChildrenConcurrentWindows(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	marker := filepath.Join(outside, "marker")
	require.NoError(t, os.WriteFile(marker, []byte("untouched"), 0o600))
	var children []path.Component
	for i := range 16 {
		name := strconv.Itoa(i)
		children = append(children, path.MustNewComponent(name))
		require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(root, name, "file"), nil, 0o600))
	}
	// Only the full sweep removes these siblings; the other callers remove
	// the numbered children. Cleanup must finish without following the link.
	require.NoError(t, os.WriteFile(filepath.Join(root, "surviving"), nil, 0o600))
	createRemovalJunction(t, filepath.Join(root, "outside"), outside)

	var directories [5]filesystem.DirectoryCloser
	for i := range directories {
		directories[i] = openRemovalDirectory(t, root)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var errors [len(directories)]error
	for i, d := range directories {
		wg.Go(func() {
			<-start
			if i == 0 {
				errors[i] = d.RemoveAllChildren()
				return
			}
			for _, child := range children {
				if err := d.RemoveAll(child); err != nil {
					errors[i] = err
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}

	remaining, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, remaining)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
}

func TestLocalDirectoryEnterWindowsLinks(t *testing.T) {
	for _, kind := range []string{"junction", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			d := openRemovalDirectory(t, root)
			require.NoError(t, os.WriteFile(filepath.Join(outside, "marker"), nil, 0o600))
			link := filepath.Join(root, "link")
			if kind == "junction" {
				createRemovalJunction(t, link, outside)
			} else {
				require.NoError(t, os.Symlink(outside, link))
			}
			subdirectory, err := d.EnterDirectory(path.MustNewComponent("link"))
			if kind == "symlink" {
				require.ErrorIs(t, err, syscall.ENOTDIR)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, subdirectory.Close()) })
			entries, err := subdirectory.ReadDir()
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, "marker", entries[0].Name().String())
		})
	}
}

func TestLocalDirectoryWindowsEmptyDirectory(t *testing.T) {
	d := openRemovalDirectory(t, t.TempDir())
	entries, err := d.ReadDir()
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, d.RemoveAllChildren())
	child := path.MustNewComponent("child")
	require.NoError(t, d.Mkdir(child, 0o700))
	require.NoError(t, d.Remove(child))
	require.NoError(t, d.Mkdir(child, 0o700))
	require.NoError(t, d.RemoveAll(child))
}

func TestLocalDirectoryWindowsNestedRemovalError(t *testing.T) {
	for _, operation := range []string{"RemoveAll", "RemoveAllChildren"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			parent := filepath.Join(root, "first", "second")
			require.NoError(t, os.MkdirAll(parent, 0o700))
			file := filepath.Join(parent, "file")
			require.NoError(t, os.WriteFile(file, []byte("protected"), 0o600))
			d := openRemovalDirectory(t, root)
			// Deletion is allowed through either DELETE on the file or
			// FILE_DELETE_CHILD on its parent, so deny both.
			denyRemovalAccess(t, file, "SD")
			denyRemovalAccess(t, parent, "0x00000040")
			var err error
			var failedPath string
			if operation == "RemoveAll" {
				err = d.RemoveAll(path.MustNewComponent("first"))
				failedPath = "second/file"
			} else {
				err = d.RemoveAllChildren()
				failedPath = "first/second/file"
			}
			require.EqualError(t, err, fmt.Sprintf("rpc error: code = Unknown desc = Failed to remove %q: %s", failedPath, windows.ERROR_ACCESS_DENIED))
			data, err := os.ReadFile(file)
			require.NoError(t, err)
			require.Equal(t, "protected", string(data))
		})
	}
}

func TestLocalDirectoryWindowsNestedReopenPermissionDenied(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "first", "second")
	require.NoError(t, os.MkdirAll(parent, 0o700))
	file := filepath.Join(parent, "file")
	require.NoError(t, os.WriteFile(file, []byte("protected"), 0o600))
	d := openRemovalDirectory(t, root)
	denyRemovalAccess(t, parent, "0x00000001")
	require.EqualError(t, d.RemoveAllChildren(), fmt.Sprintf("rpc error: code = Unknown desc = Failed to read contents of directory %q: %s", "first/second", windows.ERROR_ACCESS_DENIED))
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "protected", string(data))
}

func TestLocalDirectoryRemoveAllMissingWindows(t *testing.T) {
	d := openRemovalDirectory(t, t.TempDir())
	require.NoError(t, d.RemoveAll(path.MustNewComponent("missing")))
}

func TestLocalDirectoryWindowsClosedHandle(t *testing.T) {
	for _, operation := range []string{"ReadDir", "RemoveAllChildren", "RemoveAll", "Sync"} {
		t.Run(operation, func(t *testing.T) {
			d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(t.TempDir()))
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
	// The retained handle does not grant the new FILE_LIST_DIRECTORY access.
	denyRemovalAccess(t, root, "0x00000001")
	before, err := windows.GetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	_, err = d.ReadDir()
	require.True(t, os.IsPermission(err))
	require.True(t, os.IsPermission(d.RemoveAllChildren()))
	after, err := windows.GetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	require.Equal(t, before.String(), after.String())
}

func openRemovalDirectory(t *testing.T, root string) filesystem.DirectoryCloser {
	t.Helper()
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	return directory
}

func denyRemovalAccess(t *testing.T, name, rights string) {
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
