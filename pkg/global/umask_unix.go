// +build darwin freebsd linux

package global

import (
	"syscall"
)

func setUmask(umask uint32) error {
	syscall.Umask(int(umask))
	return nil
}
