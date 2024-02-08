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

func (pw *relativeScopeWalker) OnAbsolute() (ComponentWalker, error) {
	return nil, status.Error(codes.InvalidArgument, "Path is absolute, while a relative path was expected")
}

func (pw *relativeScopeWalker) OnRelative() (ComponentWalker, error) {
	return pw.componentWalker, nil
}
