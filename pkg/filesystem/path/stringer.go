package path

// WindowsPathFormat represents different format options for Windows path strings.
type WindowsPathFormat int

const (
	// WindowsPathFormatStandard represents the standard Windows path format.
	WindowsPathFormatStandard WindowsPathFormat = iota
	// WindowsPathFormatDevicePath represents a Windows NT device path format.
	// Note that not all paths can be printed like this (e.g. relative paths
	// cannot), so this will fallback to WindowsPathFormatStandard.
	WindowsPathFormatDevicePath
)

// Stringer is implemented by path types in this package that can be
// converted to string representations.
type Stringer interface {
	GetUNIXString() string
	GetWindowsString(format WindowsPathFormat) (string, error)
}
