//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package filesystem

import (
	"golang.org/x/sys/unix"
)

// DeviceNumber stores a block or character device number, both as
// major/minor pair and the raw value. This is done because conversion
// between both formats is platform dependent and not always bijective.
type DeviceNumber struct {
	major, minor uint32
	raw          uint64
}

// NewDeviceNumberFromMajorMinor creates a new device number based on a
// major/minor pair.
func NewDeviceNumberFromMajorMinor(major, minor uint32) DeviceNumber {
	return DeviceNumber{
		major: major,
		minor: minor,
		raw:   unix.Mkdev(major, minor),
	}
}

// NewDeviceNumberFromRaw creates a new device number based on a raw
// value.
func NewDeviceNumberFromRaw(raw uint64) DeviceNumber {
	return DeviceNumber{
		major: unix.Major(raw),
		minor: unix.Minor(raw),
		raw:   raw,
	}
}

// ToMajorMinor returns the major/minor pair of the device number.
func (d DeviceNumber) ToMajorMinor() (uint32, uint32) {
	return d.major, d.minor
}

// ToRaw returns the raw value of the device number.
func (d DeviceNumber) ToRaw() uint64 {
	return d.raw
}
