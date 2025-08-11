package path

import (
	"strings"

	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type windowsFormat struct{}

func (windowsFormat) NewParser(path string) Parser {
	return windowsParser{path: path}
}

func (windowsFormat) GetString(s Stringer) (string, error) {
	return s.GetWindowsString()
}

// WindowsFormat is capable of parsing Windows-style pathname strings, and
// stringifying parsed paths in that format as well.
var WindowsFormat Format = windowsFormat{}

func stripWindowsSeparators(p string) string {
	for p != "" && (p[0] == '/' || p[0] == '\\') {
		p = p[1:]
	}
	return p
}

type windowsParser struct {
	path string
}

func (p windowsParser) ParseScope(scopeWalker ScopeWalker) (next ComponentWalker, remainder RelativeParser, err error) {
	if len(p.path) >= 2 {
		upperDriveLetter := p.path[0] &^ 0x20
		if upperDriveLetter >= 'A' && upperDriveLetter <= 'Z' && p.path[1] == ':' {
			next, err = scopeWalker.OnDriveLetter(rune(upperDriveLetter))
			if err != nil {
				return nil, nil, err
			}
			return next, windowsRelativeParser{stripWindowsSeparators(p.path[2:])}, nil
		}

		if (p.path[0] == '\\' || p.path[0] == '/') && (p.path[1] == '\\' || p.path[1] == '/') {
			serverStart := 2
			serverLen := strings.IndexAny(p.path[serverStart:], "\\/")
			if serverLen == -1 {
				return nil, nil, status.Error(codes.InvalidArgument, "Invalid UNC path: expected a non-empty server and share name")
			}
			if serverLen < 1 {
				return nil, nil, status.Error(codes.InvalidArgument, "Invalid UNC path: expected a non-empty server name")
			}
			// This is a UNC-style path. UNC paths are formatted as:
			//   \\server\share\path
			// The format is not very well-specified, so we are tolerant
			// to slashes in either direction.
			server := p.path[serverStart : serverStart+serverLen]
			shareStart := serverStart + serverLen + 1
			shareLen := strings.IndexAny(p.path[shareStart:], "\\/")
			if shareLen < 1 {
				return nil, nil, status.Error(codes.InvalidArgument, "Invalid UNC path: expected a non-empty share name")
			}
			share := p.path[shareStart : shareStart+shareLen]
			remainder := p.path[shareStart+shareLen+1:]
			next, err = scopeWalker.OnShare(server, share)
			if err != nil {
				return nil, nil, err
			}
			return next, windowsRelativeParser{remainder}, nil
		}
	}

	if len(p.path) >= 1 && (p.path[0] == '\\' || p.path[0] == '/') {
		next, err = scopeWalker.OnAbsolute()
		if err != nil {
			return nil, nil, err
		}
		return next, windowsRelativeParser{stripWindowsSeparators(p.path)}, nil
	}

	next, err = scopeWalker.OnRelative()
	if err != nil {
		return nil, nil, err
	}
	return next, windowsRelativeParser{p.path}, nil
}

type windowsRelativeParser struct {
	path string
}

func (rp windowsRelativeParser) ParseFirstComponent(componentWalker ComponentWalker, mustBeDirectory bool) (next GotDirectoryOrSymlink, remainder RelativeParser, err error) {
	var name string
	if separator := strings.IndexAny(rp.path, "/\\"); separator == -1 {
		// Path no longer contains a separator. Consume it entirely.
		name = rp.path
		remainder = nil
	} else {
		name = rp.path[:separator]
		remainder = windowsRelativeParser{
			path: stripWindowsSeparators(rp.path[separator:]),
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

	if err := validateWindowsComponent(name); err != nil {
		return nil, nil, util.StatusWrapf(err, "Invalid pathname component %#v", name)
	}

	// A filename that was followed by a separator, or we are
	// symlink expanding one or more paths that are followed by a
	// separator. This component must yield a directory or symlink.
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
