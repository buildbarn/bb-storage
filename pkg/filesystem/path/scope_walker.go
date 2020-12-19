package path

// ScopeWalker is an interface that is called into by Resolve(). An
// implementation can use it to capture the path that is resolved.
// ScopeWalker is called into once for every path that is processed.
type ScopeWalker interface {
	// OnScope is called right before processing the first component
	// in the path (if any). The absolute argument indicates whether
	// the provided path is absolute (i.e., starting with one or
	// more slashes).
	//
	// This function can be used by the implementation to determine
	// whether path resolution needs to be relative to the current
	// directory (e.g., working directory or parent directory of the
	// previous symlink encountered) or the root directory.
	OnScope(absolute bool) (ComponentWalker, error)
}
