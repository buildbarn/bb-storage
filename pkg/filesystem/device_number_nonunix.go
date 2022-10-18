//go:build windows
// +build windows

package filesystem

// DeviceNumber stores a block or character device number as major/minor
// pair.
type DeviceNumber struct {
	major, minor uint32
}

// NewDeviceNumberFromMajorMinor creates a new device number based on a
// major/minor pair.
func NewDeviceNumberFromMajorMinor(major, minor uint32) DeviceNumber {
	return DeviceNumber{
		major: major,
		minor: minor,
	}
}

// ToMajorMinor returns the major/minor pair of the device number.
func (d DeviceNumber) ToMajorMinor() (uint32, uint32) {
	return d.major, d.minor
}
