package path

// ScopeWalker is an interface that is called into by Resolve(). An
// implementation can use it to capture the path that is resolved.
// ScopeWalker is called into once for every path that is processed.
type ScopeWalker interface {
	// One of these functions is called right before processing the
	// first component in the path (if any). Based on the
	// characteristics of the path. Absolute paths are handled through
	// OnAbsolute(), and relative paths require OnRelative(). On Windows
	// absolute paths can also start with a drive letter, which is handled
	// through OnDriveLetter(), or as a UNC path, which is handled through
	// OnShare.
	//
	// These functions can be used by the implementation to determine
	// whether path resolution needs to be relative to the current
	// directory (e.g., working directory or parent directory of the
	// previous symlink encountered) or the root directory.
	//
	// For every instance of ScopeWalker, one of OnAbsolute() or
	// OnRelative() may be called at most once. Resolve() will always
	// call into one of the interface functions for every ScopeWalker
	// presented, though decorators such as
	// VirtualRootScopeWalkerFactory may only call it when the path is
	// known to be valid. Absence of calls to OnAbsolute() or
	// OnRelative() are used to indicate that the provided path does not
	// resolve to a location inside the file
	// system.
	OnAbsolute() (ComponentWalker, error)
	OnRelative() (ComponentWalker, error)
	OnDriveLetter(drive rune) (ComponentWalker, error)
	OnShare(server, share string) (ComponentWalker, error)
}
