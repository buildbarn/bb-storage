package path

// Stringer is implemented by path types in this package that can be
// converted to string representations.
type Stringer interface {
	GetUNIXString() string
	GetWindowsString() (string, error)
}
