//go:build linux
// +build linux

package global

import (
	"golang.org/x/sys/unix"
)

var resourceLimitNames = map[string]int{
	"AS":      unix.RLIMIT_AS,
	"MEMLOCK": unix.RLIMIT_MEMLOCK,
	"NOFILE":  unix.RLIMIT_NOFILE,
	"NPROC":   unix.RLIMIT_NPROC,
	"RSS":     unix.RLIMIT_RSS,
}

type resourceLimitValueType = uint64
