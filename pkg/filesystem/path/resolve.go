package path

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func stripOneOrMoreSlashes(p string) string {
	for {
		p = p[1:]
		if p == "" || p[0] != '/' {
			return p
		}
	}
}

type resolverState struct {
	stack           []string
	componentWalker ComponentWalker
}

// Push a new path string onto the stack of paths that need to be
// processed. This happens once when the resolution process starts, and
// will happen for every symlink encountered.
func (rs *resolverState) push(scopeWalker ScopeWalker, path string) error {
	// Unix-style paths are generally passed to system calls that
	// accept C strings. There is no way these can accept null
	// bytes.
	if strings.ContainsRune(path, '\x00') {
		return status.Error(codes.InvalidArgument, "Path contains a null byte")
	}

	// Determine whether the path is absolute.
	absolute := false
	if path != "" && path[0] == '/' {
		path = stripOneOrMoreSlashes(path)
		absolute = true
	}

	// Push the path without any leading slashes onto the stack, so
	// that its components may be processed. Apply  them against the
	// right directory.
	var componentWalker ComponentWalker
	var err error
	if absolute {
		componentWalker, err = scopeWalker.OnAbsolute()
	} else {
		componentWalker, err = scopeWalker.OnRelative()
	}
	if err != nil {
		return err
	}
	rs.stack = append(rs.stack, path)
	rs.componentWalker = componentWalker
	return nil
}

// Pop a single filename from the stack of paths that need to be
// processed. This filename is the first one that should be processed.
func (rs *resolverState) pop() string {
	p := &rs.stack[len(rs.stack)-1]
	slash := strings.IndexByte(*p, '/')
	if slash == -1 {
		// Path no longer contains a slash. Consume it entirely.
		name := *p
		rs.stack = rs.stack[:len(rs.stack)-1]
		return name
	}

	// Consume the next component and as many slashes as possible.
	name := (*p)[:slash]
	*p = stripOneOrMoreSlashes((*p)[slash:])
	return name
}

func (rs *resolverState) resolve() error {
	for len(rs.stack) > 0 {
		switch name := rs.pop(); name {
		case "", ".":
			// An explicit "." entry, or an empty component.
			// Empty components can occur if paths end with
			// one or more slashes. Treat "foo/" identical
			// to "foo/."
		case "..":
			// Traverse to the parent directory.
			componentWalker, err := rs.componentWalker.OnUp()
			if err != nil {
				return err
			}
			rs.componentWalker = componentWalker
		default:
			if len(rs.stack) > 0 {
				// A filename that was followed by a
				// slash, or we are symlink expanding
				// one or more paths that are followed
				// by a slash. This component must yield
				// a directory or symlink.
				r, err := rs.componentWalker.OnDirectory(Component{
					name: name,
				})
				if err != nil {
					return err
				}
				switch rv := r.(type) {
				case GotDirectory:
					rs.componentWalker = rv.Child
				case GotSymlink:
					if err := rs.push(rv.Parent, rv.Target); err != nil {
						return err
					}
				default:
					panic("Missing result")
				}
			} else {
				// This component may be any kind of file.
				r, err := rs.componentWalker.OnTerminal(Component{
					name: name,
				})
				if err != nil || r == nil {
					// Path resolution ended with
					// any file other than a symlink.
					return err
				}
				// Observed a symlink at the end of a
				// path. We should continue to run.
				if err := rs.push(r.Parent, r.Target); err != nil {
					return err
				}
			}
		}
	}

	// Path resolution ended in a directory.
	return nil
}

// Resolve a Unix-style pathname string, similar to how the namei()
// function would work in the kernel. For every productive component in
// the pathname, a call against a ScopeWalker or ComponentWalker object
// is made. This object is responsible for registering the path
// traversal and returning symbolic link contents.
//
// This function only implements the core algorithm for path resolution.
// Features like symlink loop detection, chrooting, etc. should all be
// implemented as decorators for ScopeWalker and ComponentWalker.
func Resolve(path string, scopeWalker ScopeWalker) error {
	state := resolverState{}
	if err := state.push(scopeWalker, path); err != nil {
		return err
	}
	return state.resolve()
}
