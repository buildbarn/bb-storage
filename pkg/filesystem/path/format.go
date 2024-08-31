package path

// Format of pathname strings.
type Format interface {
	NewParser(path string) Parser
	GetString(s Stringer) (string, error)
}
