//go:build unix

package path

// LocalFormat is capable of parsing pathname strings that are in the
// format that is supported by the current operating system, and
// stringifying parsed paths in that format as well.
var LocalFormat = UNIXFormat
