//go:build freebsd
// +build freebsd

package global

import (
	"golang.org/x/sys/unix"
)

var resourceLimitNames = map[string]int{
	"AS":      unix.RLIMIT_AS,
	"CORE":    unix.RLIMIT_CORE,
	"CPU":     unix.RLIMIT_CPU,
	"DATA":    unix.RLIMIT_DATA,
	"FSIZE":   unix.RLIMIT_FSIZE,
	"MEMLOCK": unix.RLIMIT_MEMLOCK,
	"NOFILE":  unix.RLIMIT_NOFILE,
	"NPROC":   unix.RLIMIT_NPROC,
	"RSS":     unix.RLIMIT_RSS,
	"STACK":   unix.RLIMIT_STACK,
}

type resourceLimitValueType = int64
