package util

import (
	"fmt"
)

// StringList is a simple list of strings that can be used as a command
// line flag type.
type StringList []string

func (i *StringList) String() string {
	return fmt.Sprintf("%#v", *i)
}

// Set (append) an additional string value.
func (i *StringList) Set(value string) error {
	*i = append(*i, value)
	return nil
}
