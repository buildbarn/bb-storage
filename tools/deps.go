package tools

import (
	// Used by //:buildifier.
	_ "github.com/bazelbuild/buildtools/buildifier"
	// Used by CI.
	_ "golang.org/x/lint"
	// Used by CI.
	_ "mvdan.cc/gofumpt"
)
