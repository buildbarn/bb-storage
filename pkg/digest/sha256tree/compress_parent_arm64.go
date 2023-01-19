package sha256tree

func init() {
	// Always assume that the ARMv8 cryptography extension is available.
	//
	// TODO: Is there a proper way to detect this at runtime?
	// golang.org/x/sys/cpu provides this capability, but it seems
	// non-functional on macOS.
	compressParent = compressParentARM64
}

//go:noescape
func compressParentARM64(left, right, output *[Size / 4]uint32)
