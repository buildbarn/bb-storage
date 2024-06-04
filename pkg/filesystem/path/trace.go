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

var _ Stringer = &Trace{}

// Append a pathname component. The original trace is left intact.
func (t *Trace) Append(component Component) *Trace {
	return &Trace{
		parent:    t,
		component: component,
	}
}

func (t *Trace) writeToUNIXStringBuilder(sb *strings.Builder) {
	if t.parent != nil {
		t.parent.writeToUNIXStringBuilder(sb)
		sb.WriteByte('/')
	}
	sb.WriteString(t.component.String())
}

func (t *Trace) writeToWindowsStringBuilder(sb *strings.Builder) error {
	if t.parent != nil {
		if err := t.parent.writeToWindowsStringBuilder(sb); err != nil {
			return err
		}
		sb.WriteByte('\\')
	}

	if err := validateWindowsPathComponent(t.component.String()); err != nil {
		return err
	}

	sb.WriteString(t.component.String())
	return nil
}

// GetUNIXString returns a string representation of the path for use on
// UNIX-like operating systems.
func (t *Trace) GetUNIXString() string {
	if t == nil {
		return "."
	}
	var sb strings.Builder
	t.writeToUNIXStringBuilder(&sb)
	return sb.String()
}

// GetWindowsString returns a string representation of the path for use on Windows.
func (t *Trace) GetWindowsString() (string, error) {
	if t == nil {
		return ".", nil
	}

	var sb strings.Builder
	if err := t.writeToWindowsStringBuilder(&sb); err != nil {
		return "", err
	}
	return sb.String(), nil
}
