package path

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unixFormat struct{}

func (unixFormat) NewParser(path string) Parser {
	return unixParser{path}
}

func (unixFormat) GetString(s Stringer) (string, error) {
	return s.GetUNIXString(), nil
}

// UNIXFormat is capable of parsing UNIX-style pathname strings, and
// stringifying parsed paths in that format as well.
var UNIXFormat Format = unixFormat{}

func stripUNIXSeparators(p string) string {
	for {
		p = p[1:]
		if p == "" || p[0] != '/' {
			return p
		}
	}
}

type unixParser struct {
	path string
}

func (p unixParser) ParseScope(scopeWalker ScopeWalker) (next ComponentWalker, remainder RelativeParser, err error) {
	// Unix-style paths are generally passed to system calls that
	// accept C strings. There is no way these can accept null
	// bytes.
	if strings.ContainsRune(p.path, '\x00') {
		return nil, nil, status.Error(codes.InvalidArgument, "Path contains a null byte")
	}

	if p.path != "" && p.path[0] == '/' {
		next, err = scopeWalker.OnAbsolute()
		if err != nil {
			return nil, nil, err
		}

		return next, unixRelativeParser{stripUNIXSeparators(p.path)}, nil
	}

	next, err = scopeWalker.OnRelative()
	if err != nil {
		return nil, nil, err
	}

	return next, unixRelativeParser{p.path}, nil
}

type unixRelativeParser struct {
	path string
}

func (rp unixRelativeParser) ParseFirstComponent(componentWalker ComponentWalker, mustBeDirectory bool) (next GotDirectoryOrSymlink, remainder RelativeParser, err error) {
	var name string
	if slash := strings.IndexByte(rp.path, '/'); slash == -1 {
		// Path no longer contains a slash. Consume it entirely.
		name = rp.path
		remainder = nil
	} else {
		name = rp.path[:slash]
		remainder = unixRelativeParser{
			path: stripUNIXSeparators(rp.path[slash:]),
		}
	}

	switch name {
	case "", ".":
		// An explicit "." entry, or an empty component. Empty
		// components can occur if paths end with one or more
		// slashes. Treat "foo/" as identical to "foo/."
		return GotDirectory{Child: componentWalker}, remainder, nil
	case "..":
		// Traverse to the parent directory.
		parent, err := componentWalker.OnUp()
		if err != nil {
			return nil, nil, err
		}
		return GotDirectory{Child: parent}, remainder, nil
	}

	// A filename that was followed by a slash, or we are symlink
	// expanding one or more paths that are followed by a slash.
	// This component must yield a directory or symlink.
	if mustBeDirectory || remainder != nil {
		r, err := componentWalker.OnDirectory(Component{
			name: name,
		})
		if err != nil {
			return nil, nil, err
		}
		next = r
		return next, remainder, nil
	}

	r, err := componentWalker.OnTerminal(Component{
		name: name,
	})
	if err != nil || r == nil {
		// Path resolution ended with any file other than a
		// symlink.
		return nil, nil, err
	}

	// Observed a symlink at the end of a path. We should continue
	// to run.
	return GotSymlink{
		Parent: r.Parent,
		Target: r.Target,
	}, nil, nil
}
