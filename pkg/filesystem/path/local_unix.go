//go:build unix

package path

// NewLocalParser creates a pathname parser for paths that are native to
// the locally running operating system.
func NewLocalParser(path string) Parser {
	return NewUNIXParser(path)
}

// GetLocalString converts a path to a string representation that is
// supported by the locally running operating system.
func GetLocalString(s Stringer) (string, error) {
	return s.GetUNIXString(), nil
}
