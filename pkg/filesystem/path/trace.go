package path

import (
	"strings"
)

// Trace of a path.
//
// This type can be used to construct normalized relative pathnames.
// Traces are immutable, though it is possible to append pathname
// components to them.
//
// A nil pointer corresponds to path ".".
type Trace struct {
	parent    *Trace
	component Component
}

// Append a pathname component. The original trace is left intact.
func (t *Trace) Append(component Component) *Trace {
	return &Trace{
		parent:    t,
		component: component,
	}
}

func (t *Trace) writeToStringBuilder(sb *strings.Builder) {
	if t.parent != nil {
		t.parent.writeToStringBuilder(sb)
		sb.WriteByte('/')
	}
	sb.WriteString(t.component.String())
}

func (t *Trace) String() string {
	if t == nil {
		return "."
	}
	var sb strings.Builder
	t.writeToStringBuilder(&sb)
	return sb.String()
}
