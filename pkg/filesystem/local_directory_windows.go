// +build windows

package filesystem

import (
	"errors"
	"internal/syscall/windows"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const uncPrefix = `\\?\`

type localDirectory struct {
	longAbsPath string
}

func validateFilename(name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.ContainsRune(name, '/') || strings.ContainsRune(name, '\\') {
		return status.Errorf(codes.InvalidArgument, "Invalid filename: %#v", name)
	}
	return nil
}

func fileModeToFileType(mode os.FileMode) FileType {
	var fileType FileType
	if mode.IsDir() {
		fileType = FileTypeDirectory
	} else if mode&os.ModeSymlink != 0 {
		fileType = FileTypeSymlink
	} else if mode.IsRegular() {
		// Can't distinguish between executable or not, choose one.
		// TODO: Does executable help in case running Wine on Linux
		fileType = FileTypeExecutableFile
	} else {
		fileType = FileTypeOther
	}
	return fileType
}

// NewLocalDirectory creates a directory handle that corresponds to a
// local path on the system.
func NewLocalDirectory(path string) (Directory, error) {
	path = strings.ReplaceAll(path, "/", string(os.PathSeparator))
	longAbsPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(longAbsPath, uncPrefix) {
		longAbsPath = uncPrefix + longAbsPath
	}
	return newLocalDirectoryLongAbsPath(longAbsPath)
}

func newLocalDirectoryLongAbsPath(longAbsPath string) (Directory, error) {
	fileInfo, err := os.Lstat(longAbsPath)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}
	d := &localDirectory{
		longAbsPath: longAbsPath,
	}
	return d, nil
}

func (d *localDirectory) validateFilenameAndJoin(name string) (string, error) {
	if err := validateFilename(name); err != nil {
		return "", err
	}
	return filepath.Join(d.longAbsPath, name), nil
}

func (d *localDirectory) Enter(name string) (Directory, error) {
	fullPath, err := d.validateFilenameAndJoin(name)
	if err != nil {
		return nil, err
	}
	return newLocalDirectoryLongAbsPath(fullPath)
}

func (d *localDirectory) Close() error {
	return nil
}

func (d *localDirectory) Link(oldName string, newDirectory Directory, newName string) error {
	fullOldPath, err := d.validateFilenameAndJoin(oldName)
	if err != nil {
		return err
	}

	d2, ok := newDirectory.(*localDirectory)
	if !ok {
		return errors.New("Source and target directory have different types")
	}
	fullNewPath, err := d2.validateFilenameAndJoin(newName)
	if err != nil {
		return err
	}

	return os.Link(fullOldPath, fullNewPath)
}

func (d *localDirectory) Lstat(name string) (FileInfo, error) {
	fullPath, err := d.validateFilenameAndJoin(name)
	if err != nil {
		return FileInfo{}, err
	}

	fileInfo, err := os.Lstat(fullPath)
	if err != nil {
		return FileInfo{}, err
	}
	fileType := fileModeToFileType(fileInfo.Mode())
	return NewFileInfo(name, fileType), nil
}

func (d *localDirectory) Mkdir(name string, perm os.FileMode) error {
	fullPath, err := d.validateFilenameAndJoin(name)
	if err != nil {
		return err
	}

	err = os.Mkdir(fullPath, perm)
	if err != nil {
		if fileInfo, err2 := os.Stat(fullPath); err2 == nil && fileInfo.IsDir() {
			// Already exists, possibly with other casing.
			return nil
		}
	}
	return err
}

func (d *localDirectory) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	fullPath, err := d.validateFilenameAndJoin(name)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(fullPath, flag, perm)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (d *localDirectory) ReadDir() ([]FileInfo, error) {
	osFileInfoList, err := ioutil.ReadDir(d.longAbsPath)
	if err != nil {
		return nil, err
	}
	fileInfoList := make([]FileInfo, len(osFileInfoList))
	for i, osFileInfo := range osFileInfoList {
		fileType := fileModeToFileType(osFileInfo.Mode())
		fileInfoList[i] = NewFileInfo(osFileInfo.Name(), fileType)
	}
	return fileInfoList, nil
}

func (d *localDirectory) Readlink(name string) (string, error) {
	fullPath, err := d.validateFilenameAndJoin(name)
	if err != nil {
		return "", err
	}
	windowsLink, err := os.Readlink(fullPath)
	if err != nil {
		if errPath, ok := err.(*os.PathError); ok {
			if errWin, ok := errPath.Err.(syscall.Errno); ok && errWin == 0x1126 { // ERROR_NOT_A_REPARSE_POINT
				// TODO: Should this be translated? The tests are currently expecting syscall.EINVAL.
				return "", syscall.EINVAL
			}
		}
		return "", err
	}
	windowsLink = strings.TrimPrefix(windowsLink, uncPrefix)
	posixLink := strings.ReplaceAll(windowsLink, string(os.PathSeparator), "/")
	return posixLink, nil
}

func retryRemoveOnSharingViolation(op func(p string) error, path string) error {
	var err error
	for i := 0; i < 20; i++ {
		err = op(path)
		if err == nil {
			break
		} else if err == windows.ERROR_SHARING_VIOLATION {
			// Retry
			log.Print("Retrying to remove ", path, ": ", err)
			time.Sleep(100 * time.Millisecond)
		} else {
			// Unknown error
			break
		}
	}
	return err
}
func (d *localDirectory) Remove(name string) error {
	fullPath, err := d.validateFilenameAndJoin(name)
	if err != nil {
		return err
	}
	return retryRemoveOnSharingViolation(os.Remove, fullPath)
}

func (d *localDirectory) RemoveAllChildren() error {
	children, err := d.ReadDir()
	if err != nil {
		return err
	}
	for _, child := range children {
		name := child.Name()
		fullPath := filepath.Join(d.longAbsPath, name)
		err := retryRemoveOnSharingViolation(os.RemoveAll, fullPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *localDirectory) RemoveAll(name string) error {
	fullPath, err := d.validateFilenameAndJoin(name)
	if err != nil {
		return err
	}
	return retryRemoveOnSharingViolation(os.RemoveAll, fullPath)
}

func (d *localDirectory) Symlink(oldName string, newName string) error {
	fullNewPath, err := d.validateFilenameAndJoin(newName)
	if err != nil {
		return err
	}
	// Symlinking on Windows requires raised privileges. Use junctions instead.
	// os.Symlink(oldName, fullNewPath)
	var absTarget string
	if filepath.IsAbs(oldName) {
		absTarget = oldName
	} else {
		absTarget = filepath.Join(d.longAbsPath, oldName)
	}
	return MakeDirectoryJunction(fullNewPath, absTarget)
}
