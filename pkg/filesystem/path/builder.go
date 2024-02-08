package path

import (
	"strings"
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
	components           []string
	firstReversibleIndex int
	suffix               string
}

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

func (b *Builder) String() string {
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

func (b *Builder) addTrailingSlash() {
	if len(b.components) == 0 {
		// An empty path. Ensure we either emit a "/" or ".",
		// depending on whether the path is absolute.
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
		absolute:   true,
		components: w.b.components[:0],
		suffix:     "/",
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
