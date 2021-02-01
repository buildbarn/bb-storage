package path

import (
	"strings"
)

// Component of a pathname. This type is nothing more than a string that
// is guaranteed to be a valid Unix filename.
type Component struct {
	name string
}

// NewComponent creates a new pathname component. Creation fails in case
// the name is empty, ".", "..", contains a slash, or is not a valid
// C string.
func NewComponent(name string) (Component, bool) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return Component{}, false
	}
	return Component{name: name}, true
}

// MustNewComponent is identical to NewComponent, except that it panics
// upon failure.
func MustNewComponent(name string) Component {
	c, ok := NewComponent(name)
	if !ok {
		panic("Invalid component name")
	}
	return c
}

func (c Component) String() string {
	return c.name
}
