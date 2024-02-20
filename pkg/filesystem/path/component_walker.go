package path

// GotDirectory is returned by ComponentWalker.OnDirectory(), in case
// the name corresponds to a directory stored in the current directory.
type GotDirectory struct {
	// Child directory against which resolution should continue.
	Child ComponentWalker

	// Whether Child.OnUp() is guaranteed to refer to the same
	// directory. This information can be used by Builder to remove
	// unnecessary ".." pathname components. This should be set to
	// false if paths are not resolved against a concrete file
	// system to ensure ".." components remain present.
	IsReversible bool
}

// GotSymlink is returned by ComponentWalker.OnDirectory() and
// OnTerminal(), in case the name corresponds to a symbolic link stored
// in the current directory.
type GotSymlink struct {
	// The parent directory of the symbolic link, which is relative
	// to where symlink expansion needs to be performed.
	Parent ScopeWalker

	// The contents of the symbolic link.
	Target Parser
}

// GotDirectoryOrSymlink is a union type of GotDirectory and GotSymlink.
// It is returned by ComponentWalker.OnDirectory(), as that function may
// return either a direcgory or symbolic link.
type GotDirectoryOrSymlink interface {
	isGotDirectoryOrSymlink()
}

func (GotDirectory) isGotDirectoryOrSymlink() {}
func (GotSymlink) isGotDirectoryOrSymlink()   {}

// ComponentWalker is an interface that is called into by Resolve(). An
// implementation can use it to capture the path that is resolved.
// ComponentWalker is called into after determining whether the path is
// absolute or relative (see ScopeWalker). It is called into once for
// every pathname component that is processed.
//
// Each of ComponentWalker's methods invalidate the object on which it
// is called. Additional calls must be directed against the ScopeWalkers
// and ComponentWalkers yielded by these methods.
type ComponentWalker interface {
	// OnDirectory is called for every pathname component that must
	// resolve to a directory or a symbolic link to a directory.
	//
	// If the pathname component refers to a directory, this function
	// will return a GotDirectory containing a new ComponentWalker
	// against which successive pathname components can be resolved.
	//
	// If the pathname component refers to a symbolic link, this
	// function will return a GotSymlink containing a ScopeWalker, which
	// can be used to perform expansion of the symbolic link. The
	// Resolve() function will call into OnAbsolute() or OnRelative() to
	// signal whether resolution should continue at the root directory
	// or at the directory that contained the symbolic link.
	OnDirectory(name Component) (GotDirectoryOrSymlink, error)

	// OnTerminal is called for the potentially last pathname
	// component that needs to be resolved. It may resolve to any
	// kind of file.
	//
	// If the pathname component does not refer to a symbolic link,
	// the function will return nil. This causes Resolve() to assume
	// that pathname resolution has completed.
	//
	// If the pathname component refers to a symbolic link, this
	// function will return a GotSymlink, just like OnDirectory()
	// does. This causes Resolve() to continue the resolution
	// process.
	OnTerminal(name Component) (*GotSymlink, error)

	// OnUp is called if a ".." pathname component is observed.
	OnUp() (ComponentWalker, error)
}

// OnTerminalViaOnDirectory is an implementation of ComponentWalker's
// OnTerminal() that just calls into OnDirectory(). This is sufficient
// for implementations of ComponentWalker that only support resolving
// directories and symbolic links.
func OnTerminalViaOnDirectory(cw ComponentWalker, name Component) (*GotSymlink, error) {
	r, err := cw.OnDirectory(name)
	if err != nil {
		return nil, err
	}
	switch rv := r.(type) {
	case GotDirectory:
		return nil, nil
	case GotSymlink:
		return &rv, nil
	default:
		panic("Missing result")
	}
}

// TerminalNameTrackingComponentWalker can be embedded into an
// implementation of ComponentWalker to provide a default implementation
// of the OnTerminal() method. OnTerminal() is implemented in such a way
// that it simply tracks the name.
//
// This implementation is useful for ComponentWalkers that are used to
// create new files or directories.
type TerminalNameTrackingComponentWalker struct {
	TerminalName *Component
}

// OnTerminal records the name of the final component of a path.
func (cw *TerminalNameTrackingComponentWalker) OnTerminal(name Component) (*GotSymlink, error) {
	cw.TerminalName = &name
	return nil, nil
}
