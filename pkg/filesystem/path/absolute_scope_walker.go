package path

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type absoluteScopeWalker struct {
	componentWalker ComponentWalker
}

// NewAbsoluteScopeWalker creates a ScopeWalker that only accepts
// absolute paths.
func NewAbsoluteScopeWalker(componentWalker ComponentWalker) ScopeWalker {
	return &absoluteScopeWalker{
		componentWalker: componentWalker,
	}
}

func (pw *absoluteScopeWalker) OnRelative() (ComponentWalker, error) {
	return nil, status.Error(codes.InvalidArgument, "Path is relative, while an absolute path was expected")
}

func (pw *absoluteScopeWalker) OnAbsolute() (ComponentWalker, error) {
	return pw.componentWalker, nil
}
