package path

import (
	"strings"

	"github.com/buildbarn/bb-storage/pkg/util"
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

func (t *Trace) writeToStringBuilder(separator byte, sb *strings.Builder) {
	if t.parent != nil {
		t.parent.writeToStringBuilder(separator, sb)
		sb.WriteByte(separator)
	}
	sb.WriteString(t.component.String())
}

// GetUNIXString returns a string representation of the path for use on
// UNIX-like operating systems.
func (t *Trace) GetUNIXString() string {
	if t == nil {
		return "."
	}
	var sb strings.Builder
	t.writeToStringBuilder('/', &sb)
	return sb.String()
}

// GetWindowsString returns a string representation of the path for use
// on Windows.
func (t *Trace) GetWindowsString(format WindowsPathFormat) (string, error) {
	if t == nil {
		return ".", nil
	}

	// Ensure that we only emit paths with filenames that are valid
	// on Windows.
	for tValidate := t; tValidate != nil; tValidate = tValidate.parent {
		componentStr := tValidate.component.String()
		if err := validateWindowsComponent(componentStr); err != nil {
			return "", util.StatusWrapf(err, "Invalid pathname component %#v", componentStr)
		}
	}

	var sb strings.Builder
	t.writeToStringBuilder('\\', &sb)
	return sb.String(), nil
}

// ToList returns the pathname components contained in the trace to a list.
func (t *Trace) ToList() []Component {
	count := 0
	for tCount := t; tCount != nil; tCount = tCount.parent {
		count++
	}

	components := make([]Component, count)
	for count > 0 {
		count--
		components[count] = t.component
		t = t.parent
	}
	return components
}
