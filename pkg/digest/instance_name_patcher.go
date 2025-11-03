package digest

// InstanceNamePatcher can be used to insert, strip or change a prefix
// of an instance name in InstanceName and Digest objects.
type InstanceNamePatcher interface {
	PatchInstanceName(i InstanceName) InstanceName

	PatchDigest(d Digest) Digest
	UnpatchDigest(d Digest) Digest
}

type noopInstanceNamePatcher struct{}

func (noopInstanceNamePatcher) PatchInstanceName(i InstanceName) InstanceName {
	return i
}

func (noopInstanceNamePatcher) PatchDigest(d Digest) Digest {
	return d
}

func (noopInstanceNamePatcher) UnpatchDigest(d Digest) Digest {
	return d
}

// NoopInstanceNamePatcher is an InstanceNamePatcher that performs no
// substitutions on the instance name.
var NoopInstanceNamePatcher InstanceNamePatcher = noopInstanceNamePatcher{}

type actualInstanceNamePatcher struct {
	oldPrefixWithSlash    string
	oldPrefixWithoutSlash string
	newPrefixWithSlash    string
	newPrefixWithoutSlash string
}

// NewInstanceNamePatcher creates an InstanceNamePatcher that replaces a
// given prefix of an instance name with another value. It is not valid
// to apply the resulting InstanceNamePatcher against objects that don't
// have an instance name starting with the provided prefix.
func NewInstanceNamePatcher(oldPrefix, newPrefix InstanceName) InstanceNamePatcher {
	// Prevent unnecessary string allocations in case the
	// substitutions have no effect.
	if oldPrefix == newPrefix {
		return NoopInstanceNamePatcher
	}

	ip := &actualInstanceNamePatcher{
		oldPrefixWithoutSlash: oldPrefix.String(),
		newPrefixWithoutSlash: newPrefix.String(),
	}
	ip.oldPrefixWithSlash = ip.oldPrefixWithoutSlash
	if ip.oldPrefixWithSlash != "" {
		ip.oldPrefixWithSlash += "/"
	}
	ip.newPrefixWithSlash = ip.newPrefixWithoutSlash
	if ip.newPrefixWithSlash != "" {
		ip.newPrefixWithSlash += "/"
	}
	return ip
}

func patchInstanceName(i string, oldPrefixWithSlashLength int, newPrefixWithSlash, newPrefixWithoutSlash string) string {
	if len(i) > oldPrefixWithSlashLength {
		// The instance name starts with the old prefix, but
		// also has trailing pathname components. Replace the
		// prefix with the new prefix followed by a slash.
		return newPrefixWithSlash + i[oldPrefixWithSlashLength:]
	}
	// The instance name is identical to the old prefix, without any
	// trailing pathname components. Replace it with the new
	// instance name.
	return newPrefixWithoutSlash
}

func (ip *actualInstanceNamePatcher) PatchInstanceName(i InstanceName) InstanceName {
	return InstanceName{
		value: patchInstanceName(i.value, len(ip.oldPrefixWithSlash), ip.newPrefixWithSlash, ip.newPrefixWithoutSlash),
	}
}

func patchDigest(d Digest, oldPrefixWithSlashLength int, newPrefixWithSlash, newPrefixWithoutSlash string) Digest {
	_, _, _, sizeBytesEnd := d.unpack()
	instanceNameStart := sizeBytesEnd + 1
	return Digest{
		value: d.value[:instanceNameStart] + patchInstanceName(d.value[instanceNameStart:], oldPrefixWithSlashLength, newPrefixWithSlash, newPrefixWithoutSlash),
	}
}

func (ip *actualInstanceNamePatcher) PatchDigest(d Digest) Digest {
	return patchDigest(d, len(ip.oldPrefixWithSlash), ip.newPrefixWithSlash, ip.newPrefixWithoutSlash)
}

func (ip *actualInstanceNamePatcher) UnpatchDigest(d Digest) Digest {
	return patchDigest(d, len(ip.newPrefixWithSlash), ip.oldPrefixWithSlash, ip.oldPrefixWithoutSlash)
}
