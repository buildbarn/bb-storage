package path

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type relativeScopeWalker struct {
	componentWalker ComponentWalker
}

// NewRelativeScopeWalker creates a ScopeWalker that only accepts
// relative paths.
func NewRelativeScopeWalker(componentWalker ComponentWalker) ScopeWalker {
	return &relativeScopeWalker{
		componentWalker: componentWalker,
	}
}

func (pw *relativeScopeWalker) OnScope(absolute bool) (ComponentWalker, error) {
	if absolute {
		return nil, status.Error(codes.InvalidArgument, "Path is absolute, while a relative path was expected")
	}
	return pw.componentWalker, nil
}
