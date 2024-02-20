package path



// Parser is used by Resolve to parse paths in the resolution. Implementations
// of ParseScope() should return a new copy of Parser and leave the current
// instance unmodified. It is permitted to call ParseScope() multiple times.
type Parser interface {
	ParseScope(scopeWalker ScopeWalker) (next ComponentWalker, remainder RelativeParser, err error)
}

// RelativeParser is used by Resolve to parse relative paths in the resolution.
// Implementations of ParseFirstComponent() should return a new copy of Parser
// and leave the current instance unmodified. It is permitted to call
// ParseFirstComponent() multiple times.
type RelativeParser interface {
	ParseFirstComponent(componentWalker ComponentWalker, mustBeDirectory bool) (next GotDirectoryOrSymlink, remainder RelativeParser, err error)
}
