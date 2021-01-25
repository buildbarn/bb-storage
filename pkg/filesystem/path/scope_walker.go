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
	//
	// For every instance of ScopeWalker, OnScope() may be called at
	// most once. Resolve() will always call into OnScope() for
	// every ScopeWalker presented, though decorators such as
	// VirtualRootScopeWalkerFactory may only call it when the path
	// is known to be valid. Absence of calls to OnScope() are used
	// to indicate that the provided path does not resolve to a
	// location inside the file system.
	OnScope(absolute bool) (ComponentWalker, error)
}
