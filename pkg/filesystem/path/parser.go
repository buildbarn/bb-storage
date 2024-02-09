package path

// Parser is used by Resolve to parse paths in the resolution.
type Parser interface {
	ParseScope(scopeWalker ScopeWalker) (next ComponentWalker, remainder RelativeParser, err error)
}

// RelativeParser is used by Resolve to parse relative paths in the resolution.
type RelativeParser interface {
	ParseFirstComponent(componentWalker ComponentWalker, mustBeDirectory bool) (next GotDirectoryOrSymlink, remainder RelativeParser, err error)
}
