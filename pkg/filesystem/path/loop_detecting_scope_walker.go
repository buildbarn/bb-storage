package path

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type loopDetectingScopeWalker struct {
	base         ScopeWalker
	symlinksLeft int
}

// NewLoopDetectingScopeWalker creates a decorator for ScopeWalker that
// prevents unlimited expansion of symbolic links by only allowing a
// finite number of iterations.
//
// This implementation permits up to 40 iterations, which is the same as
// what Linux supports (MAXSYMLINKS in include/linux/namei.h).
func NewLoopDetectingScopeWalker(base ScopeWalker) ScopeWalker {
	return &loopDetectingScopeWalker{
		base:         base,
		symlinksLeft: 40,
	}
}

func (w *loopDetectingScopeWalker) OnAbsolute() (ComponentWalker, error) {
	componentWalker, err := w.base.OnAbsolute()
	if err != nil {
		return nil, err
	}
	return &loopDetectingComponentWalker{
		base:         componentWalker,
		symlinksLeft: w.symlinksLeft,
	}, nil
}

func (w *loopDetectingScopeWalker) OnDriveLetter(drive rune) (ComponentWalker, error) {
	componentWalker, err := w.base.OnDriveLetter(drive)
	if err != nil {
		return nil, err
	}
	return &loopDetectingComponentWalker{
		base:         componentWalker,
		symlinksLeft: w.symlinksLeft,
	}, nil
}

func (w *loopDetectingScopeWalker) OnRelative() (ComponentWalker, error) {
	componentWalker, err := w.base.OnRelative()
	if err != nil {
		return nil, err
	}
	return &loopDetectingComponentWalker{
		base:         componentWalker,
		symlinksLeft: w.symlinksLeft,
	}, nil
}

func (w *loopDetectingScopeWalker) OnShare(server, share string) (ComponentWalker, error) {
	componentWalker, err := w.base.OnShare(server, share)
	if err != nil {
		return nil, err
	}
	return &loopDetectingComponentWalker{
		base:         componentWalker,
		symlinksLeft: w.symlinksLeft,
	}, nil
}

type loopDetectingComponentWalker struct {
	base         ComponentWalker
	symlinksLeft int
}

func (cw *loopDetectingComponentWalker) wrapComponentWalker(child ComponentWalker) ComponentWalker {
	return &loopDetectingComponentWalker{
		base:         child,
		symlinksLeft: cw.symlinksLeft,
	}
}

func (cw *loopDetectingComponentWalker) wrapGotSymlink(r GotSymlink) (GotSymlink, error) {
	if cw.symlinksLeft == 0 {
		return GotSymlink{}, status.Error(codes.InvalidArgument, "Maximum number of symbolic link redirections reached")
	}
	r.Parent = &loopDetectingScopeWalker{
		base:         r.Parent,
		symlinksLeft: cw.symlinksLeft - 1,
	}
	return r, nil
}

func (cw *loopDetectingComponentWalker) OnDirectory(name Component) (GotDirectoryOrSymlink, error) {
	r, err := cw.base.OnDirectory(name)
	if err != nil {
		return nil, err
	}
	switch rv := r.(type) {
	case GotDirectory:
		rv.Child = cw.wrapComponentWalker(rv.Child)
		return rv, nil
	case GotSymlink:
		return cw.wrapGotSymlink(rv)
	default:
		panic("Missing result")
	}
}

func (cw *loopDetectingComponentWalker) OnTerminal(name Component) (*GotSymlink, error) {
	r, err := cw.base.OnTerminal(name)
	if err != nil || r == nil {
		return nil, err
	}
	nr, err := cw.wrapGotSymlink(*r)
	if err != nil {
		return nil, err
	}
	return &nr, nil
}

func (cw *loopDetectingComponentWalker) OnUp() (ComponentWalker, error) {
	parent, err := cw.base.OnUp()
	if err != nil {
		return nil, err
	}
	return cw.wrapComponentWalker(parent), nil
}
