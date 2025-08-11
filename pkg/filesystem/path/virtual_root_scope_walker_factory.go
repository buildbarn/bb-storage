package path

import (
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// namelessVirtualRootNode contains the bookkeeping of
// VirtualRootScopeWalkerFactory for every nameless path (i.e., "/",
// paths ending with "..").
type namelessVirtualRootNode struct {
	up     *namelessVirtualRootNode
	down   map[Component]*namedVirtualRootNode
	isRoot bool
}

// namedVirtualRootNode contains the bookkeeping of
// VirtualRootScopeWalkerFactory for every named path. These are the
// paths at which aliases may be placed. These become symbolic links
// that point into the root directory.
type namedVirtualRootNode struct {
	nameless namelessVirtualRootNode
	target   Parser
}

// virtualRootNodeCreator is an implementation of ComponentWalker that
// is used by NewVirtualRootScopeWalkerFactory() to create
// namelessVirtualRootNode and namedVirtualRootNode entries to register
// the root path and its aliases.
type virtualRootNodeCreator struct {
	namelessNode *namelessVirtualRootNode
	namedNode    *namedVirtualRootNode
}

func (cw *virtualRootNodeCreator) OnDirectory(name Component) (GotDirectoryOrSymlink, error) {
	n, ok := cw.namelessNode.down[name]
	if ok {
		if n.nameless.isRoot || n.target != nil {
			return nil, status.Error(codes.InvalidArgument, "Path resides at or below an already registered path")
		}
	} else {
		n = &namedVirtualRootNode{
			nameless: namelessVirtualRootNode{
				down: map[Component]*namedVirtualRootNode{},
			},
		}
		cw.namelessNode.down[name] = n
	}
	cw.namelessNode = &n.nameless
	cw.namedNode = n
	return GotDirectory{
		Child:        cw,
		IsReversible: false,
	}, nil
}

func (cw *virtualRootNodeCreator) OnTerminal(name Component) (*GotSymlink, error) {
	return OnTerminalViaOnDirectory(cw, name)
}

func (cw *virtualRootNodeCreator) OnUp() (ComponentWalker, error) {
	n := cw.namelessNode.up
	if n == nil {
		n = &namelessVirtualRootNode{
			down: map[Component]*namedVirtualRootNode{},
		}
		cw.namelessNode.up = n
	}
	cw.namelessNode = n
	cw.namedNode = nil
	return cw, nil
}

// VirtualRootScopeWalkerFactory is a factory for decorators for
// ScopeWalker that place them at a path inside of a virtual file system
// hierarchy, potentially consisting of one or more symlinks pointing to
// it.
//
// This type can be used to ensure symlink expansion works properly in
// case the root of the directory hierarchy that is walked is in reality
// not the root directory of the system. In this hierarchy there may be
// symlinks that contain absolute target paths. These can only be
// resolved properly by trimming one or more leading pathname
// components.
//
// The underlying implementation of ScopeWalker has the ability to
// detect whether the resolved path lies inside or outside of the nested
// directory hierarchy by monitoring whether a ScopeWalker interface
// method has been called. If this function is called on the wrapped
// ScopeWalker, but not called on the underlying instance (either
// initially or after returning a GotSymlink response), the resolved
// path lies outside the nested root directory.
type VirtualRootScopeWalkerFactory struct {
	rootNode namelessVirtualRootNode
}

// NewVirtualRootScopeWalkerFactory creates a
// VirtualRootScopeWalkerFactory. The rootPath argument denotes an
// absolute path at which the underlying ScopeWalker should be placed in
// the virtual file system hierarchy. The aliases argument is a map of
// absolute paths to relative paths. These are converted to symbolic
// links that are placed inside the virtual root, pointing to relative
// locations underneath the ScopeWalker.
//
// For example, if this function is called with rootPath == "/root" and
// aliases == {"/alias": "target"}, then paths on the outer ScopeWalker
// resolve to the following locations in the underlying ScopeWalker:
//
//	"hello"        -> "hello"
//	"/root"        -> "/"
//	"/root/hello"  -> "/hello"
//	"/alias"       -> "/target"
//	"/alias/hello" -> "/target/hello"
//	"/"            -> Nothing
//	"/hello"       -> Nothing
func NewVirtualRootScopeWalkerFactory(rootPath Parser, aliases map[string]string) (*VirtualRootScopeWalkerFactory, error) {
	wf := &VirtualRootScopeWalkerFactory{
		rootNode: namelessVirtualRootNode{
			down: map[Component]*namedVirtualRootNode{},
		},
	}

	// Resolve the directory at which we want to place the virtual root.
	rootCreator := virtualRootNodeCreator{namelessNode: &wf.rootNode}
	rootPathBuilder, rootPathWalker := EmptyBuilder.Join(
		NewAbsoluteScopeWalker(&rootCreator))

	if err := Resolve(rootPath, rootPathWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve root path")
	}
	rootCreator.namelessNode.isRoot = true

	for alias, target := range aliases {
		// Resolve the location at which we want to create a fictive
		// symlink that points into the virtual root directory.
		aliasCreator := virtualRootNodeCreator{namelessNode: &wf.rootNode}
		if err := Resolve(UNIXFormat.NewParser(alias), NewAbsoluteScopeWalker(&aliasCreator)); err != nil {
			return nil, util.StatusWrapf(err, "Failed to resolve alias path %#v", alias)
		}
		if aliasCreator.namedNode == nil {
			return nil, status.Errorf(codes.InvalidArgument, "Failed to resolve alias path %#v: Last component is not a valid filename", alias)
		}
		if aliasCreator.namelessNode.up != nil || len(aliasCreator.namelessNode.down) > 0 {
			return nil, status.Errorf(codes.InvalidArgument, "Failed to resolve alias path %#v: Path resides above an already registered path", alias)
		}

		// Convert the relative target path to an absolute path
		// underneath the virtual root.
		targetPathBuilder, targetPathWalker := rootPathBuilder.Join(
			NewRelativeScopeWalker(VoidComponentWalker))
		if err := Resolve(UNIXFormat.NewParser(target), targetPathWalker); err != nil {
			return nil, util.StatusWrapf(err, "Failed to resolve alias target %#v", target)
		}
		aliasCreator.namedNode.target = targetPathBuilder
	}
	return wf, nil
}

// New wraps an existing ScopeWalker to place it inside the virtual root.
func (wf *VirtualRootScopeWalkerFactory) New(base ScopeWalker) ScopeWalker {
	return &virtualRootScopeWalker{
		base:     base,
		rootNode: &wf.rootNode,
	}
}

// virtualRootScopeWalker is VirtualRootScopeWalkerFactory's decorator
// for ScopeWalker objects.
type virtualRootScopeWalker struct {
	base     ScopeWalker
	rootNode *namelessVirtualRootNode
}

func (w *virtualRootScopeWalker) getComponentWalker(n *namelessVirtualRootNode) (ComponentWalker, error) {
	if !n.isRoot {
		// There are still one or more pathname components that
		// need to be processed until we reach the underlying
		// root directory or a symlink.
		return &pendingVirtualRootComponentWalker{
			walker:      w,
			currentNode: n,
		}, nil
	}

	// We've reached the underlying root directory.
	root, err := w.base.OnAbsolute()
	if err != nil {
		return nil, err
	}
	return &finishedVirtualRootComponentWalker{
		base:     root,
		rootNode: w.rootNode,
	}, nil
}

func (w *virtualRootScopeWalker) OnAbsolute() (ComponentWalker, error) {
	return w.getComponentWalker(w.rootNode)
}

func (w *virtualRootScopeWalker) OnDriveLetter(drive rune) (ComponentWalker, error) {
	return w.getComponentWalker(w.rootNode)
}

func (w *virtualRootScopeWalker) OnRelative() (ComponentWalker, error) {
	// Attempted to resolve a relative path. There is no need to
	// rewrite any paths. Do wrap the ComponentWalker to ensure
	// future symlinks respect the virtual root.
	base, err := w.base.OnRelative()
	if err != nil {
		return nil, err
	}
	return &finishedVirtualRootComponentWalker{
		base:     base,
		rootNode: w.rootNode,
	}, nil
}

func (w *virtualRootScopeWalker) OnShare(server, share string) (ComponentWalker, error) {
	return w.getComponentWalker(w.rootNode)
}

// pendingVirtualRootComponentWalker is VirtualRootScopeWalkerFactory's
// decorator for ComponentWalker when resolution takes place inside the
// virtual root directory (i.e., outside the underlying root directory).
type pendingVirtualRootComponentWalker struct {
	walker      *virtualRootScopeWalker
	currentNode *namelessVirtualRootNode
}

func (cw *pendingVirtualRootComponentWalker) OnDirectory(name Component) (GotDirectoryOrSymlink, error) {
	n := cw.currentNode.down[name]
	if n == nil {
		// Attempted to enter an unknown path. Simply let
		// resolution continue while abandoning the underlying
		// root directory. This allows Builder to still capture
		// the full path.
		return VoidComponentWalker.OnDirectory(name)
	}
	if n.target != nil {
		// Found an alias to the underlying root directory.
		return GotSymlink{
			Parent: cw.walker,
			Target: n.target,
		}, nil
	}
	child, err := cw.walker.getComponentWalker(&n.nameless)
	if err != nil {
		return nil, err
	}
	return GotDirectory{
		Child:        child,
		IsReversible: false,
	}, nil
}

func (cw *pendingVirtualRootComponentWalker) OnTerminal(name Component) (*GotSymlink, error) {
	return OnTerminalViaOnDirectory(cw, name)
}

func (cw *pendingVirtualRootComponentWalker) OnUp() (ComponentWalker, error) {
	n := cw.currentNode.up
	if n == nil {
		return VoidComponentWalker.OnUp()
	}
	return cw.walker.getComponentWalker(n)
}

// finishedVirtualRootComponentWalker is VirtualRootScopeWalkerFactory's
// decorator for ComponentWalker when resolution takes place inside the
// underlying root directory (i.e., outside the virtual root directory).
//
// The only purpose of this decorator is to ensure that ScopeWalker
// objects returned as part of GotSymlink responses are decorated as
// well. This is needed to ensure that symbolic links with absolute
// target paths respect the virtual root.
type finishedVirtualRootComponentWalker struct {
	base     ComponentWalker
	rootNode *namelessVirtualRootNode
}

func (cw *finishedVirtualRootComponentWalker) wrapComponentWalker(base ComponentWalker) ComponentWalker {
	return &finishedVirtualRootComponentWalker{
		base:     base,
		rootNode: cw.rootNode,
	}
}

func (cw *finishedVirtualRootComponentWalker) wrapGotSymlink(r GotSymlink) GotSymlink {
	r.Parent = &virtualRootScopeWalker{
		base:     r.Parent,
		rootNode: cw.rootNode,
	}
	return r
}

func (cw *finishedVirtualRootComponentWalker) OnDirectory(name Component) (GotDirectoryOrSymlink, error) {
	r, err := cw.base.OnDirectory(name)
	if err != nil {
		return nil, err
	}
	switch rv := r.(type) {
	case GotDirectory:
		rv.Child = cw.wrapComponentWalker(rv.Child)
		return rv, nil
	case GotSymlink:
		return cw.wrapGotSymlink(rv), nil
	default:
		panic("Missing result")
	}
}

func (cw *finishedVirtualRootComponentWalker) OnTerminal(name Component) (*GotSymlink, error) {
	r, err := cw.base.OnTerminal(name)
	if err != nil || r == nil {
		return nil, err
	}
	nr := cw.wrapGotSymlink(*r)
	return &nr, nil
}

func (cw *finishedVirtualRootComponentWalker) OnUp() (ComponentWalker, error) {
	componentWalker, err := cw.base.OnUp()
	if err != nil {
		return nil, err
	}
	return cw.wrapComponentWalker(componentWalker), nil
}
