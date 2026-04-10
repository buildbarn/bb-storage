//go:build !linux && !freebsd

package blockdevice

func allocateFile(fd int, sizeBytes int64) error {
	return nil
}

func syncDataRange(fd int, off, nbytes int64) error {
	return nil
}
