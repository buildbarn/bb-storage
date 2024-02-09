package path

type resolverState struct {
	stack           []RelativeParser
	componentWalker ComponentWalker
}

// Push a new path onto the stack of paths that need to be
// processed. This happens once when the resolution process starts, and
// will happen for every symlink encountered.
func (rs *resolverState) push(scopeWalker ScopeWalker, parser Parser) error {
	// Push the path without any leading slashes onto the stack, so
	// that its components may be processed. Apply them against the
	// right directory.
	componentWalker, remainder, err := parser.ParseScope(scopeWalker)
	if err != nil {
		return err
	}
	rs.stack = append(rs.stack, remainder)
	rs.componentWalker = componentWalker
	return nil
}

// Pop a single filename from the stack of paths that need to be
// processed. This filename is the first one that should be processed.
func (rs *resolverState) pop() (GotDirectoryOrSymlink, error) {
	p := rs.stack[len(rs.stack)-1]
	node, remainder, err := p.ParseFirstComponent(rs.componentWalker, len(rs.stack) > 1)
	if err != nil {
		return nil, err
	}
	if remainder == nil {
		rs.stack = rs.stack[:len(rs.stack)-1]
	} else {
		rs.stack[len(rs.stack)-1] = remainder
	}
	return node, err
}

func (rs *resolverState) resolve() error {
	for len(rs.stack) > 0 {
		r, err := rs.pop()
		if err != nil {
			return err
		}
		switch rv := r.(type) {
		case GotDirectory:
			rs.componentWalker = rv.Child
		case GotSymlink:
			target, err := NewUNIXParser(rv.Target)
			if err != nil {
				return err
			}
			if err := rs.push(rv.Parent, target); err != nil {
				return err
			}
		default:
			panic("Missing result")
		case nil: // Do nothing
		}
	}

	// Path resolution ended in a directory.
	return nil
}

// Resolve a pathname string, similar to how the namei() function would
// work in the kernel. For every productive component in the pathname, a
// call against a ScopeWalker or ComponentWalker object is made. This
// object is responsible for registering the path traversal and
// returning symbolic link contents. Unix-style paths can be created
// with NewUNIXParser.
//
// This function only implements the core algorithm for path resolution.
// Features like symlink loop detection, chrooting, etc. should all be
// implemented as decorators for ScopeWalker and ComponentWalker.
func Resolve(parser Parser, scopeWalker ScopeWalker) error {
	state := resolverState{}
	if err := state.push(scopeWalker, parser); err != nil {
		return err
	}
	return state.resolve()
}
