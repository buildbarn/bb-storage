package path

import (
	"strings"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// Builder for normalized pathname strings.
//
// Instead of providing its own API for constructing paths, every
// Builder is created with an associated decorator for ScopeWalker. This
// means that Builder can, for example, be used to record the path
// traversed by Resolve(), similar to Go's filepath.EvalSymlinks() and
// libc's realpath().
//
// If there is no need to take the state of the file system into
// account, it's possible to let the Builder decorate VoidScopeWalker.
// This allows the construction of paths that don't exist (yet). In that
// case, unnecessary ".." components are retained, as preceding pathname
// components may refer to symlinks when applied against an actual file
// system.
type Builder struct {
	absolute             bool
	driveLetter          rune
	components           []string
	firstReversibleIndex int
	server               string
	share                string
	suffix               string
}

var (
	_ Parser   = &Builder{}
	_ Stringer = &Builder{}
)

// EmptyBuilder is a Builder that contains path ".". New instances of
// Builder that use this path as their starting point can be created by
// calling EmptyBuilder.Join().
var EmptyBuilder = Builder{
	suffix: ".",
}

// RootBuilder is a Builder that contains path "/". New instances of
// Builder that use this path as their starting point can be created by
// calling RootBuilder.Join().
var RootBuilder = Builder{
	absolute: true,
	suffix:   "/",
}

// GetUNIXString returns a string representation of the path for use on
// UNIX-like operating systems.
func (b *Builder) GetUNIXString() string {
	// Emit pathname components.
	prefix := ""
	if b.absolute {
		prefix = "/"
	}
	var out strings.Builder
	for _, component := range b.components {
		out.WriteString(prefix)
		out.WriteString(component)
		prefix = "/"
	}

	// Emit trailing slash in case the path refers to a directory,
	// or a dot or slash if the path is empty.
	out.WriteString(b.suffix)
	return out.String()
}

// GetWindowsString returns a string representation of the path for use on
// Windows.
func (b *Builder) GetWindowsString() (string, error) {
	// Emit pathname components.
	var out strings.Builder
	prefix := ""
	if b.driveLetter != 0 {
		out.WriteString(string(b.driveLetter))
		out.WriteString(":")
		prefix = "\\"
	} else if b.server != "" {
		out.WriteString(`\\`)
		out.WriteString(b.server)
		out.WriteString(`\`)
		out.WriteString(b.share)
		prefix = "\\"
	} else if b.absolute {
		prefix = "\\"
	}

	for _, component := range b.components {
		if err := validateWindowsComponent(component); err != nil {
			return "", util.StatusWrapf(err, "Invalid pathname component %#v", component)
		}

		out.WriteString(prefix)
		out.WriteString(component)
		prefix = "\\"
	}

	// Emit trailing slash in case the path refers to a directory,
	// or a dot or slash if the path is empty. The suffix is been
	// constructed by platform-independent code that uses forward
	// slashes. To construct a Windows path we must use a
	// backslash.
	suffix := b.suffix
	if suffix == "/" {
		suffix = "\\"
	}
	out.WriteString(suffix)
	return out.String(), nil
}

func (b *Builder) addTrailingSlash() {
	if len(b.components) == 0 {
		// An empty path. Ensure we either emit a "/" or ".",
		// depending on whether the path is absolute/drive letter.
		if b.absolute {
			b.suffix = "/"
		} else {
			b.suffix = "."
		}
	} else if b.components[len(b.components)-1] == ".." {
		// There is no need to put a trailing slash behind a
		// ".." component, as there is no way that can resolve
		// to a regular file.
		b.suffix = ""
	} else {
		b.suffix = "/"
	}
}

func (b *Builder) getScopeWalker(base ScopeWalker) ScopeWalker {
	return &buildingScopeWalker{
		base: base,
		b:    b,
	}
}

func (b *Builder) getComponentWalker(base ComponentWalker) ComponentWalker {
	return &buildingComponentWalker{
		base: base,
		b:    b,
	}
}

// ParseScope is provided, so that Builder implements the Parser
// interface. This makes it possible to pass instances of Builder
// directly to Resolve(). This can be used to replay resolution of a
// previously constructed path.
func (b *Builder) ParseScope(scopeWalker ScopeWalker) (next ComponentWalker, remainder RelativeParser, err error) {
	if b.driveLetter != 0 {
		next, err = scopeWalker.OnDriveLetter(b.driveLetter)
	} else if b.server != "" {
		next, err = scopeWalker.OnShare(b.server, b.share)
	} else if b.absolute {
		next, err = scopeWalker.OnAbsolute()
	} else {
		next, err = scopeWalker.OnRelative()
	}
	if err != nil {
		return nil, nil, err
	}
	return next, builderRelativeParser{
		components:      b.components,
		lastIsDirectory: b.suffix != "",
	}, nil
}

// Join another path with the results computed thus far.
//
// This function returns a copy of Builder and ScopeWalker that can be
// used to compute a path relative to the path computed thus far. If the
// newly provided path is relative, it is concatenated to the existing
// path. A trailing slash is appended to the original path. This is done
// to enforce that the original path is a directory.
//
// If the newly provided path is absolute, it replaces the original path
// entirely. If this needs to be prevented, it's possible to provide a
// ScopeWalker that was created using NewRelativeScopeWalker().
func (b *Builder) Join(scopeWalker ScopeWalker) (*Builder, ScopeWalker) {
	newB := *b
	newB.components = append([]string(nil), b.components...)
	newB.addTrailingSlash()
	return &newB, newB.getScopeWalker(scopeWalker)
}

type buildingScopeWalker struct {
	base ScopeWalker
	b    *Builder
}

func (w *buildingScopeWalker) OnAbsolute() (ComponentWalker, error) {
	componentWalker, err := w.base.OnAbsolute()
	if err != nil {
		return nil, err
	}
	*w.b = Builder{
		absolute:    true,
		driveLetter: w.b.driveLetter,
		components:  w.b.components[:0],
		suffix:      "/",
	}
	return w.b.getComponentWalker(componentWalker), nil
}

func (w *buildingScopeWalker) OnDriveLetter(drive rune) (ComponentWalker, error) {
	componentWalker, err := w.base.OnDriveLetter(drive)
	if err != nil {
		return nil, err
	}
	*w.b = Builder{
		absolute:    true,
		driveLetter: drive,
		components:  w.b.components[:0],
		suffix:      "/",
	}
	return w.b.getComponentWalker(componentWalker), nil
}

func (w *buildingScopeWalker) OnRelative() (ComponentWalker, error) {
	componentWalker, err := w.base.OnRelative()
	if err != nil {
		return nil, err
	}
	return w.b.getComponentWalker(componentWalker), nil
}

func (w *buildingScopeWalker) OnShare(server, share string) (ComponentWalker, error) {
	componentWalker, err := w.base.OnShare(server, share)
	if err != nil {
		return nil, err
	}
	*w.b = Builder{
		absolute:   true,
		components: w.b.components[:0],
		server:     server,
		share:      share,
		suffix:     "/",
	}
	return w.b.getComponentWalker(componentWalker), nil
}

type buildingComponentWalker struct {
	base ComponentWalker
	b    *Builder
}

func (cw *buildingComponentWalker) OnDirectory(name Component) (GotDirectoryOrSymlink, error) {
	r, err := cw.base.OnDirectory(name)
	if err != nil {
		return nil, err
	}
	switch rv := r.(type) {
	case GotDirectory:
		cw.b.components = append(cw.b.components, name.String())
		if !rv.IsReversible {
			cw.b.firstReversibleIndex = len(cw.b.components)
		}
		cw.b.suffix = "/"
		rv.Child = cw.b.getComponentWalker(rv.Child)
		return rv, nil
	case GotSymlink:
		rv.Parent = cw.b.getScopeWalker(rv.Parent)
		return rv, nil
	default:
		panic("Missing result")
	}
}

func (cw *buildingComponentWalker) OnTerminal(name Component) (*GotSymlink, error) {
	r, err := cw.base.OnTerminal(name)
	if err != nil {
		return nil, err
	}
	if r == nil {
		cw.b.components = append(cw.b.components, name.String())
		cw.b.firstReversibleIndex = len(cw.b.components)
		cw.b.suffix = ""
		return nil, nil
	}
	r.Parent = cw.b.getScopeWalker(r.Parent)
	return r, nil
}

func (cw *buildingComponentWalker) OnUp() (ComponentWalker, error) {
	componentWalker, err := cw.base.OnUp()
	if err != nil {
		return nil, err
	}
	if cw.b.absolute && len(cw.b.components) == 0 {
		// Don't add ".." components if we're already at the root
		// directory. That would yield "/..", which isn't useful.
	} else if cw.b.firstReversibleIndex < len(cw.b.components) {
		// The last component is reversible, meaning that
		// appending "/.." or removing the last component yield
		// the same directory. Prefer the shorter
		// representation, but do add a trailing slash to
		// require that the resulting path is a directory.
		cw.b.components = cw.b.components[:len(cw.b.components)-1]
		cw.b.addTrailingSlash()
	} else {
		// Append a ".." component.
		cw.b.components = append(cw.b.components, "..")
		cw.b.firstReversibleIndex = len(cw.b.components)
		cw.b.suffix = ""
	}
	return cw.b.getComponentWalker(componentWalker), nil
}

// builderRelativeParser is an implementation of RelativeParser that is
// backed by the contents of a Builder. This can be used to replay
// resolution of a previously constructed path.
type builderRelativeParser struct {
	components      []string
	lastIsDirectory bool
}

func (rp builderRelativeParser) ParseFirstComponent(componentWalker ComponentWalker, mustBeDirectory bool) (next GotDirectoryOrSymlink, remainder RelativeParser, err error) {
	// Stop parsing if there are no components left.
	if len(rp.components) == 0 {
		return GotDirectory{Child: componentWalker}, nil, nil
	}
	name := rp.components[0]
	remainder = builderRelativeParser{
		components:      rp.components[1:],
		lastIsDirectory: rp.lastIsDirectory,
	}

	// Call one of OnUp(), OnDirectory() or OnTerminal(), depending
	// on the component name and its location in the path.
	if name == ".." {
		parent, err := componentWalker.OnUp()
		if err != nil {
			return nil, nil, err
		}
		return GotDirectory{Child: parent}, remainder, nil
	}

	if len(rp.components) > 1 || rp.lastIsDirectory || mustBeDirectory {
		r, err := componentWalker.OnDirectory(Component{name: name})
		return r, remainder, err
	}

	r, err := componentWalker.OnTerminal(Component{name: name})
	if err != nil || r == nil {
		return nil, nil, err
	}
	return r, nil, nil
}
